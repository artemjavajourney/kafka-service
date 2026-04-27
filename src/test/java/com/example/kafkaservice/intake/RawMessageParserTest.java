package com.example.kafkaservice.intake;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RawMessageParserTest {

    private RawMessageParser parser;

    @BeforeEach
    void setUp() {
        parser = new RawMessageParser(new ObjectMapper());
    }

    @Test
    void parse_shouldReturnParsed_whenValidNestedJsonString() {
        String raw = """
                {"loading_id":"L1","result_json":"{\\\"body\\\":{\\\"entity_type\\\":\\\"ENTITY_1\\\",\\\"trend_uuid\\\":\\\"t-1\\\"}}"}
                """;

        ParsedMessage result = parser.parse(raw);

        assertEquals(ParseStatus.PARSED, result.parseStatus());
        assertEquals("L1", result.loadingId());
        assertEquals("ENTITY_1", result.entityType());
        assertTrue(result.isBodyUsable());
    }

    @Test
    void parse_shouldReturnRawOnly_whenResultJsonMissing() {
        ParsedMessage result = parser.parse("{\"loadingId\":\"L2\"}");

        assertEquals(ParseStatus.RAW_ONLY, result.parseStatus());
        assertEquals("L2", result.loadingId());
        assertNotNull(result.errorMessage());
    }

    @Test
    void parse_shouldReturnInvalidJson_whenRawIsBroken() {
        ParsedMessage result = parser.parse("{");

        assertEquals(ParseStatus.INVALID_JSON, result.parseStatus());
        assertNull(result.loadingId());
    }

    @Test
    void parse_shouldReturnRawOnly_whenBodyIsNotObject() {
        ParsedMessage result = parser.parse("{\"result_json\":{\"body\":123}}\n");

        assertEquals(ParseStatus.RAW_ONLY, result.parseStatus());
        assertTrue(result.errorMessage().contains("body is not"));
    }

    @Test
    void parserHelpers_shouldHandleConversions() {
        var node = parser.parseJson("{\"a\":\"42\",\"b\":\"true\",\"c\":\"x\"}");

        assertEquals("42", parser.firstText(node, "a"));
        assertEquals(42, parser.firstInt(node, "a"));
        assertNull(parser.firstInt(node, "c"));
        assertTrue(parser.firstBoolean(node, "b"));
        assertNull(parser.firstBoolean(node, "c"));
        assertEquals("{\"a\":\"42\",\"b\":\"true\",\"c\":\"x\"}", parser.toJson(node));
    }
}
