# kafka-service-target

Сервис-consumer для Kafka с двухстадийной обработкой: **durable intake** и **batch apply**.

## Что делает сервис (верхнеуровнево)

Пайплайн разделён на два независимых этапа:

1. **Intake (Kafka listener)**
   - читает сообщение из Kafka;
   - парсит raw-message единым `RawMessageParser` (извлекает `loading_id`, `entity_type`, `body`, статус парсинга);
   - сохраняет только raw-пакет и Kafka-позицию в `staging_inbox`;
   - создаёт запись в `event_processing_log` со статусом `STAGED`.

2. **Apply (scheduler/batch)**
   - периодически забирает пачку `STAGED/DEFERRED` сообщений;
   - переводит их в `PROCESSING`;
   - повторно парсит raw через тот же `RawMessageParser`;
   - маршрутизирует в entity-handler по `entity_type`;
   - делает bulk-upsert в финальные таблицы;
   - обновляет `event_processing_log` в `APPLIED/SKIPPED/DEFERRED/FAILED`.

> Важно: в текущей модели БД нет FK-связей между `ENTITY_1`, `ENTITY_2`, `ENTITY_3`, поэтому apply для `ENTITY_2/ENTITY_3` выполняется **без проверки существования `ENTITY_1`**.

## Структура проекта

- `config` — запуск приложения и конфигурация модулей.
- `consumer` — Kafka listener.
- `intake` — intake orchestration + контракт результата intake.
- `message` — единый парсер raw-сообщений (`RawMessageParser`).
- `apply` — batch orchestration.
  - `apply.handler` — handlers по сущностям (`ENTITY_1/2/3`).
  - `apply.model` — DTO бизнес-сущностей.
  - `apply.support` — вспомогательные утилиты (dedupe и т.п.).
- `repository` — JDBC-репозитории `staging_inbox` и `event_processing_log`.
- `finaltable.repository` — JDBC-репозитории финальных таблиц.
- `audit` — модель и статусы журнала обработки.

## Модель хранения

### `staging_inbox`

Таблица минимальная и хранит только intake-слой:
- Kafka-координаты: `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_key`
- `raw_message`
- один timestamp: `received_at`

Удалены поля-дубликаты/неиспользуемые поля:
- `loading_id`, `entity_type`, `parse_status`, `intake_status`, `error_message`, `staged_at`

Все статусы и диагностические поля обработки живут в `event_processing_log`.

### `event_processing_log`

Единый источник правды по прогрессу обработки:
- `status` (`STAGED`, `PROCESSING`, `APPLIED`, `SKIPPED`, `DEFERRED`, `FAILED`)
- counters (`records_inserted`, `records_updated`, `records_skipped`)
- `error_message`
- `loading_id`, `entity_type`
- `created_at`, `updated_at`

## Cookbook: как добавить новую сущность (`ENTITY_N`)

Ниже минимальный checklist для первого входа в проект.

1. **Модель**
   - Добавьте DTO в `apply.model` (например, `EntityNData`).
   - Определите business key (если нужен составной ключ — отдельный `EntityNKey`).

2. **Маппинг**
   - Добавьте метод в `BusinessEntityMapper`:
     - вход: `JsonNode body`
     - выход: `EntityNData`
   - Для чтения полей используйте `RawMessageParser.firstText/firstInt/firstBoolean`.

3. **Repository финальной таблицы**
   - Создайте `finaltable.repository.EntityNRepository`:
     - `findComparableBy...(...)`
     - `batchUpsert(...)`
   - Поддержите сравнение без лишних апдейтов (`Comparable`-модель).

4. **Apply handler**
   - Создайте `apply.handler.EntityNApplyHandler`.
   - Реализуйте `supportedType()` -> `ENTITY_N`.
   - В `handle(...)`:
     - валидируйте обязательные поля ключа;
     - deduplicate внутри batch через `ApplyDedupeUtil`;
     - сравните с БД и соберите upsert-список;
     - заполните `ApplyStatusUpdate` (`inserted/updated/skipped/failed/deferred`).

5. **Подключение handler-а в orchestration**
   - Ничего вручную регистрировать не нужно: Spring подхватит `@Component`.
   - `ApplyOrchestrator` сам группирует сообщения по `supportedType()`.

6. **DDL и миграции**
   - Добавьте Liquibase changeset с созданием/изменением финальной таблицы.
   - Проверьте индексы под ключи поиска и upsert.

7. **Проверка сценария**
   - Прогоните локально intake + apply.
   - Убедитесь, что в `event_processing_log` корректно выставляются статусы.

## Runtime notes

Для запуска нужны:
- Kafka broker
- PostgreSQL
- корректный `application.yml`

Ключевой тюнинг:
- `app.apply.fixed-delay-ms` — задержка между apply-итерациями.

## Standalone fake producer для E2E

```bash
mvn -q -DskipTests org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.mainClass=com.example.kafkaservice.tools.FakeKafkaProducer \
  -Dexec.args='--bootstrap-servers localhost:9092 --topic voiceres --scenario demo --count 6'
```

Сценарии:
- `demo` (циклически `ENTITY_1 -> ENTITY_2 -> ENTITY_3`)
- `entity1`
- `entity2`
- `entity3`
