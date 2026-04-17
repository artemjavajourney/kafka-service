package com.example.kafkaservice.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Standalone utility producer for E2E local checks.
 *
 * Example:
 * java ... com.example.kafkaservice.tools.FakeKafkaProducer \
 *   --bootstrap-servers localhost:9092 --topic voiceres --scenario demo --count 5
 */
public class FakeKafkaProducer {

    public static void main(String[] args) throws Exception {
        ProducerOptions options = ProducerOptions.parse(args);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        ObjectMapper objectMapper = new ObjectMapper();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < options.count(); i++) {
                String key = options.keyPrefix() + "-" + i;
                String payload = buildPayload(options.scenario(), i, objectMapper);
                producer.send(new ProducerRecord<>(options.topic(), key, payload)).get();
                System.out.printf("Sent %s to topic=%s key=%s%n", options.scenario(), options.topic(), key);
            }
        }
    }

    private static String buildPayload(String scenario, int idx, ObjectMapper mapper) throws Exception {
        Map<String, Object> envelope = new LinkedHashMap<>();
        Map<String, Object> body = switch (scenario.toLowerCase()) {
            case "entity1" -> entity1Body(idx);
            case "entity2" -> entity2Body(idx);
            case "entity3" -> entity3Body(idx);
            case "demo" -> {
                int m = idx % 3;
                if (m == 0) {
                    yield entity1Body(idx);
                }
                if (m == 1) {
                    yield entity2Body(idx);
                }
                yield entity3Body(idx);
            }
            default -> throw new IllegalArgumentException("Unknown scenario: " + scenario);
        };

        Map<String, Object> resultJson = new LinkedHashMap<>();
        resultJson.put("body", body);

        envelope.put("result_json", resultJson);
        return mapper.writeValueAsString(envelope);
    }

    private static Map<String, Object> entity1Body(int idx) {
        String trendUuid = UUID.nameUUIDFromBytes(("trend-" + (idx / 3)).getBytes()).toString();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("entity_type", "ENTITY_1");
        body.put("trend_uuid", trendUuid);
        body.put("trend_name", "Trend #" + idx);
        body.put("emotion", idx % 5);
        body.put("is_visible", true);
        body.put("product_id", 200 + (idx % 3));
        body.put("group_id", "group-" + (idx % 2));
        body.put("is_archived", false);
        body.put("employee_id_create", "emp-" + idx);
        body.put("created_at", "2026-04-17T10:00:00Z");
        body.put("prev_product_id", 199 + (idx % 3));
        return body;
    }

    private static Map<String, Object> entity2Body(int idx) {
        String trendUuid = UUID.nameUUIDFromBytes(("trend-" + (idx / 3)).getBytes()).toString();
        String summaryUuid = UUID.nameUUIDFromBytes(("summary-" + idx).getBytes()).toString();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("entity_type", "ENTITY_2");
        body.put("cm_id", "cm-" + idx);
        body.put("answer_date", LocalDate.now().toString());
        body.put("created_at", LocalDate.now().toString());
        body.put("event_type_id", 10 + (idx % 2));
        body.put("trend_uuid", trendUuid);
        body.put("summary_uuid", summaryUuid);
        body.put("client_segment_code", "SEG_A");
        body.put("product_id", 300 + (idx % 4));
        body.put("prev_product_id", 299 + (idx % 4));
        return body;
    }

    private static Map<String, Object> entity3Body(int idx) {
        String trendUuid = UUID.nameUUIDFromBytes(("trend-" + (idx / 3)).getBytes()).toString();
        String summaryUuid = UUID.nameUUIDFromBytes(("summary-" + idx).getBytes()).toString();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("entity_type", "ENTITY_3");
        body.put("summary_uuid", summaryUuid);
        body.put("summary_name", "Summary #" + idx);
        body.put("trend_uuid", trendUuid);
        body.put("sentiment", idx % 3);
        body.put("is_etalon", idx % 2 == 0);
        body.put("employee_id_create", "emp-" + idx);
        body.put("created_at", "2026-04-17T11:00:00Z");
        return body;
    }

    private record ProducerOptions(
            String bootstrapServers,
            String topic,
            String scenario,
            int count,
            String keyPrefix
    ) {
        private static ProducerOptions parse(String[] args) {
            Map<String, String> values = new LinkedHashMap<>();
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (!arg.startsWith("--")) {
                    continue;
                }

                String key = arg.substring(2);
                String value = i + 1 < args.length ? args[++i] : "";
                values.put(key, value);
            }

            return new ProducerOptions(
                    values.getOrDefault("bootstrap-servers", "localhost:9092"),
                    values.getOrDefault("topic", "voiceres"),
                    values.getOrDefault("scenario", "demo"),
                    Integer.parseInt(values.getOrDefault("count", "3")),
                    values.getOrDefault("key-prefix", "fake")
            );
        }
    }
}
