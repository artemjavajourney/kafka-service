# Глубокий технический разбор `kafka-service` по исходному коду

## 1) Что именно реализовано

Проект реализует **двухфазный конвейер обработки Kafka-событий**:

1. **Intake-фаза (горячий путь)**
   - читает сообщения из Kafka;
   - извлекает только минимум метаданных (loading id, entity type, parse status);
   - сохраняет *сырое* сообщение в `staging_inbox`;
   - создает/поддерживает запись жизненного цикла в `event_processing_log` со статусом `STAGED`.

2. **Apply-фаза (фоновый батч-процессор)**
   - по расписанию забирает батч `STAGED/DEFERRED`;
   - переводит их в `PROCESSING`;
   - нормализует и маппит payload в типизированные модели;
   - применяет бизнес-правила по типам сущностей (`ENTITY_1`, `ENTITY_2`, `ENTITY_3`);
   - пишет в финальные таблицы `final_entity_1/2/3`;
   - завершает lifecycle-статусом `APPLIED`, `SKIPPED`, `DEFERRED` или `FAILED`.

Важно: по коду это **не stream-processing с синхронным apply на каждый record**. Применение бизнес-логики отделено и вынесено в scheduler.

## 2) Архитектура и распределение ответственности

### Точка входа и runtime-конфигурация

- `KafkaServiceApplication` включает Spring Boot, сканирование `@ConfigurationProperties` и scheduler (`@EnableScheduling`).
- `KafkaModuleProperties` биндует `app.kafka.topic`.
- `application.yml` задает Kafka consumer, datasource, Liquibase и частоту apply-цикла (`app.apply.fixed-delay-ms`).

### Слои (по фактическому коду)

- `consumer`
  - `IntegrationKafkaListener`: Kafka-слушатель, делегирует в intake-service.

- `intake`
  - `KafkaIntakeService`: транзакционное сохранение в staging + первичная запись в audit log;
  - `MessageMetadataExtractor`: best-effort парсинг envelope и `result_json`.

- `repository`
  - `StagingInboxRepository`: идемпотентная вставка в staging по `(topic, partition, offset)`;
  - `EventProcessingLogRepository`: запись/claim/батч-обновление статусов lifecycle.

- `apply`
  - `ApplyBatchScheduler`: периодический запуск apply;
  - `ApplyOrchestrator`: orchestration пайплайна apply;
  - `BusinessPayloadExtractor` + `BusinessEntityMapper`: извлечение и нормализация бизнес-данных;
  - `apply.handler/*`: per-entity бизнес-обработчики;
  - `apply.support/*`: dedupe и carrier-модель.

- `finaltable.repository`
  - JDBC upsert и compare чтение для `final_entity_1/2/3`.

- `audit`
  - lifecycle статусы и модель `EventProcessingLogRecord`.

### Наблюдаемые архитектурные подходы

1. **Staged processing / outbox-like intake boundary**
   - запись в staging + processing_log как durable граница перед бизнес-apply.

2. **Orchestrator + strategy handlers**
   - `ApplyOrchestrator` группирует по типу;
   - `ApplyEntityHandler` + реализации — стратегия по `supportedType()`.

3. **Repository pattern (JDBC)**
   - SQL сосредоточен в репозиториях;
   - сервисы не содержат SQL.

4. **At-least-once intake + idempotent persistence**
   - intake-запись защищена unique constraint + `on conflict do nothing`.

## 3) Значимые компоненты: вход → обработка → выход

### 3.1 `IntegrationKafkaListener`

**Роль:** входная Kafka-точка.

**Вход:** `ConsumerRecord<String, String>`.

**Что делает:** вызывает `kafkaIntakeService.intake(record)`.

**Выход:** `IntakeResult` создается, но далее не используется в listener.

**Взаимодействия:** `KafkaIntakeService`.

### 3.2 `KafkaIntakeService`

**Роль:** минимальный intake и durable staging.

**Вход:** Kafka record.

**Что делает (по шагам):**
1. Берет `now` в UTC.
2. Вызывает `MessageMetadataExtractor.extract(record.value())`.
3. Формирует `StagingInboxRecord` (Kafka-метаданные + raw message + parse status).
4. Делает `stagingInboxRepository.insertIfAbsent(...)`.
5. Создает/пишет lifecycle запись через `eventProcessingLogRepository.insertStagedIfAbsent(...)`.
6. Логирует staging-факт и возвращает `IntakeResult`.

**Выход:** `IntakeResult(stagingId, loadingId, entityType, parseStatus)`.

**Ошибки:** явного `try/catch` нет; откатываются транзакцией Spring (`@Transactional`).

### 3.3 `MessageMetadataExtractor`

**Роль:** best-effort разбор envelope без жесткого падения ingest.

**Вход:** raw JSON string.

**Что делает:**
- парсит корень JSON;
- ищет `loading_id/loadingId`;
- разбирает `result_json`:
  - если string — пытается распарсить как nested JSON;
  - если object — берет как есть;
  - иначе ставит `RAW_ONLY` и ошибку.
- из `inner.body` пытается достать entity type через несколько alias-полей.

**Выход:** `ExtractedMetadata(loadingId, entityType, parseStatus, errorMessage)`.

**Ошибки:**
- любые исключения в корневом парсинге превращаются в `INVALID_JSON` + `errorMessage`.

### 3.4 `StagingInboxRepository`

**Роль:** идемпотентное сохранение intake-record.

**Вход:** `StagingInboxRecord`.

**Что делает:**
- `INSERT ... ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING RETURNING id`;
- если вставки не было (duplicate), делает `SELECT id` существующей строки.

**Выход:** всегда возвращает `staging_id` существующей или новой строки.

### 3.5 `EventProcessingLogRepository`

**Роль:** lifecycle-таблица обработки.

**Ключевые операции:**
- `insertStagedIfAbsent(...)`: начальная запись `STAGED` (идемпотентно по `staging_id`).
- `claimNextBatch(batchSize, now)`:
  - выбирает `STAGED/DEFERRED`;
  - блокирует `FOR UPDATE SKIP LOCKED`;
  - атомарно переводит их в `PROCESSING`;
  - возвращает `ApplyCandidate` + `raw_message` join-ом со staging.
- `batchUpdateStatuses(...)`: батчевое обновление финальных статусов/счетчиков.

### 3.6 `ApplyBatchScheduler`

**Роль:** триггер apply-фазы.

**Вход:** таймер (`fixedDelay`).

**Что делает:** до 20 итераций подряд вызывает `applyOrchestrator.applyNextBatch()` пока есть работа; если пусто — заканчивает тик.

**Выход:** side effects в БД + логи.

### 3.7 `ApplyOrchestrator`

**Роль:** центральная оркестрация apply-batch.

**Вход:** не получает явных аргументов; сам claim-ит до `DEFAULT_BATCH_SIZE=500`.

**Что делает (детально):**
1. `claimNextBatch` из audit log (уже в `PROCESSING`).
2. Инициализирует bucket-ы кандидатов по `handler.supportedType()`.
3. Для каждого `ApplyCandidate`:
   - парсит payload (`BusinessPayloadExtractor.extract`);
   - нормализует entity type (trim + upper);
   - если handler не найден → `SKIPPED: Unknown entity type`;
   - если парсинг упал → `FAILED`.
4. Вызывает каждый handler с его списком кандидатов.
5. Сохраняет итоговые статус-обновления `batchUpdateStatuses`.
6. Логирует агрегаты (`applied`, `deferred`, `other`).

**Выход:** `int` = количество кандидатов в батче.

### 3.8 `BusinessPayloadExtractor` и `BusinessEntityMapper`

**Роль:** преобразование raw JSON в typed business records.

- `BusinessPayloadExtractor.extract(rawMessage, fallbackEntityType)`:
  - достает `result_json`; если строка — парсит как JSON;
  - берет `body`;
  - извлекает entity type или берет fallback;
  - возвращает `BusinessPayload(entityType, body)`;
  - при проблеме бросает `IllegalArgumentException`.

- `BusinessEntityMapper`:
  - `toEntityOne`, `toEntityTwo`, `toEntityThree` маппят поля и alias;
  - безопасно парсит даты (`LocalDate`/`OffsetDateTime`), при ошибке возвращает `null`.

### 3.9 Entity-handlers

#### `EntityOneApplyHandler` (`supportedType = ENTITY_1`)

- Валидирует бизнес-ключ `trend_uuid`; иначе `FAILED`.
- Делает dedupe в пределах батча по `trend_uuid` (оставляет последнюю версию, старые `SKIPPED`).
- Читает compare snapshot из `final_entity_1`.
- Решает insert/update/skip("No changes").
- Выполняет `batchUpsert`.

#### `EntityTwoApplyHandler` (`supportedType = ENTITY_2`)

- Валидирует составной ключ: `cm_id`, `trend_uuid`, `summary_uuid`, `answer_date`; при нехватке — `DEFERRED`.
- Dedupe по `EntityTwoKey`.
- Проверяет существование родителя (`ENTITY_1`) через `findExistingTrendUuids`.
  - если родителя нет — `DEFERRED`.
- Compare с `final_entity_2`, далее insert/update/skip.

#### `EntityThreeApplyHandler` (`supportedType = ENTITY_3`)

- Валидирует `summary_uuid`; иначе `FAILED`.
- Dedupe по `summary_uuid`.
- Compare с `final_entity_3`, далее insert/update/skip.

### 3.10 Final repositories

- `EntityOneRepository`, `EntityTwoRepository`, `EntityThreeRepository`:
  - батчевый upsert через `insert ... on conflict do update`;
  - lightweight read-модели `*Comparable` для принятия решения "данные изменились?".

## 4) Реальная цепочка вызова: от входа до финала

Ниже фактическая call-chain по коду.

### 4.1 Intake path

1. Kafka доставляет `ConsumerRecord` в `@KafkaListener`.
2. `IntegrationKafkaListener.listen()` вызывает `KafkaIntakeService.intake(record)`.
3. `KafkaIntakeService` извлекает метаданные (`MessageMetadataExtractor`).
4. Формирует `StagingInboxRecord` и сохраняет через `StagingInboxRepository.insertIfAbsent`.
5. Формирует `EventProcessingLogRecord.staged(...)` и пишет `insertStagedIfAbsent`.
6. Возвращает `IntakeResult`.

**Преобразование данных:** raw Kafka JSON остается raw; из него выделяются только служебные метаданные и parse status.

### 4.2 Apply path

1. `ApplyBatchScheduler.run()` по таймеру вызывает `ApplyOrchestrator.applyNextBatch()`.
2. `claimNextBatch` атомарно переводит порцию сообщений в `PROCESSING` и возвращает `ApplyCandidate(raw_message...)`.
3. Для каждого кандидата:
   - `BusinessPayloadExtractor.extract` делает parsing `result_json.body`;
   - `normalizeType` определяет bucket handler-а.
4. Handler-ы применяют типовую бизнес-логику:
   - mapping `JsonNode` -> typed DTO;
   - обязательные проверки ключей/родителей;
   - dedupe внутри батча;
   - сравнение с текущей БД;
   - upsert только если есть вставка/изменение.
5. Orchestrator вызывает `batchUpdateStatuses`.
6. Строки lifecycle получают финальные статусы.

## 5) Что относится к бизнес-логике, а что к инфраструктуре

### Бизнес-логика (по коду)

- Правила валидации и defer/fail для entity payload:
  - обязательные ключи ENTITY_1/2/3;
  - зависимость ENTITY_2 от существующего ENTITY_1;
  - правила "No changes".
- Dedupe в рамках одного батча (оставить последнюю версию ключа).
- Логика classify status: `APPLIED/SKIPPED/DEFERRED/FAILED`.

### Инфраструктура

- Kafka listener wiring, scheduling, configuration.
- JDBC repositories и SQL upsert/select/update.
- Liquibase-миграции схемы.
- Модель статусов и аудит-таблица lifecycle.

## 6) Нюансы и зоны неопределенности

1. **Коммит offset-а Kafka**
   - В `application.yml` стоит `enable-auto-commit: false`, listener `ack-mode: record`.
   - Но в коде listener-метода нет `Acknowledgment` параметра и нет явного `ack.acknowledge()`.
   - Это не позволяет из текущего кода однозначно доказать точный момент фиксации offset.
   - Формально: **это зона неопределенности, требующая проверки runtime-конфигурации контейнера/listener-factory**.

2. **Реальный порядок "ENTITY_1 до ENTITY_2" между батчами**
   - Внутри одной итерации handler-ы вызваны в порядке `@Order(1,2,3)`.
   - Но выборка кандидатов идет общим батчем по `event_processing_log` (`STAGED/DEFERRED`) без явного разделения по loading_id.
   - Сама документация в `docs` говорит про обработку по loading_id, но код claim-а это напрямую не реализует.
   - Следовательно: "apply строго по loading_id" — **не следует из кода напрямую**.

3. **Семантика dedupe "последняя версия"**
   - `ApplyDedupeUtil` использует `LinkedHashMap` и перезапись по ключу при итерации.
   - Оставляется последний элемент *в порядке списка кандидатов*.
   - Поскольку кандидаты в claim-е сортируются по `staging_id`, "последняя" = запись с большим `staging_id` внутри текущего батча.

## 7) Что можно проверить быстро при эксплуатации

- Рост `DEFERRED` у `ENTITY_2` обычно означает запаздывание родителя `ENTITY_1`.
- Повтор Kafka-сообщений с тем же `(topic, partition, offset)` не должен плодить новые staging id.
- При неизменившемся бизнес-состоянии ожидаем `SKIPPED: No changes`.

## 8) Ответы на вопросы ревью (практически, без рефакторинга ради рефакторинга)

### 8.1 Можно ли упростить проект, не ломая функциональность?

Да, но точечно:

1. Убрать «мертвые» зависимости и реально использовать `KafkaModuleProperties`.
2. Сохранить двухфазную схему intake/apply — она уже оправдана и не выглядит «лишней».
3. Не трогать ядро (staging + claim + handler-ы), потому что это функционально-значимая часть, а не «архитектурная косметика».

### 8.2 Как использовать `KafkaModuleProperties`, чтобы это не был мертвый код?

Слушатель Kafka переведен на topic через бин properties:

- `@KafkaListener(topics = "#{@kafkaModuleProperties.topic}")`

Это делает `KafkaModuleProperties` реально задействованным в runtime.

### 8.3 Для чего `parse_status`, если он нигде не меняется?

`parse_status` заполняется на intake и фиксирует качество первичного разбора (`PARSED/RAW_ONLY/INVALID_JSON`).
Это не state-machine apply, а **технический след ingest-качества**.
Даже если он не участвует в ветвлении apply, он полезен для операционной диагностики и SLA intake.

### 8.4 Для чего `IntakeStatus`, если он не участвует в state machine?

Сейчас это маркер того, что запись дошла до durable staging (`STAGED`).
На текущем коде это скорее «этапный флаг intake», чем полноценный lifecycle.
Удалять его имеет смысл только если вы полностью переносите intake-статусы в одну таблицу (`event_processing_log`) и не используете `staging_inbox.intake_status` в отчетности/диагностике.

### 8.5 Как избежать бесконечного цикла `DEFERRED`?

Практическое решение для production:

1. Ввести счетчик defer-попыток (`defer_attempts`) в `event_processing_log`.
2. При каждом повторном defer увеличивать счетчик.
3. После порога (например, 20 попыток или TTL) переводить в `FAILED` с причиной `defer timeout`.

В текущем коде это пока не реализовано — здесь действительно зона потенциального бесконечного ретрая.

### 8.6 Почему compare в `EntityOneRepository` был неполным?

Изначально сравнивались только `trend_name`, `emotion`, `product_id`, а upsert писал больше полей.
Это приводило к возможному `SKIPPED: No changes`, даже если менялись `is_visible`, `group_id`, `is_archived`, `employee_id_create`, `created_at`, `prev_product_id`.

Исправлено: compare расширен до всех upsert-полей.

### 8.7 Зачем был fallback `event_type_id` -> entity type и можно ли уйти?

Да, можно и лучше уйти, если контрактом считается, что тип сущности приходит как `entity_type`.
Fallback через `event_type_id` может давать ложную классификацию (числовой идентификатор события != тип сущности handler-а).

Исправлено: извлечение типа сущности теперь только из `entity_type/entityType`.

### 8.8 Должен ли `ENTITY_3` зависеть от `ENTITY_1`?

По прежнему коду зависимости не было.
Если доменная модель требует эту зависимость, то корректно делать как у `ENTITY_2`:

- проверять наличие `trend_uuid`,
- проверять существование родителя в `final_entity_1`,
- при отсутствии родителя переводить в `DEFERRED`.

Исправлено: `EntityThreeApplyHandler` теперь также проверяет parent `ENTITY_1`.
