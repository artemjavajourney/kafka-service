package com.example.kafkaservice.apply;

import com.example.kafkaservice.intake.RawMessageParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BusinessEntityMapperTest {

    private BusinessEntityMapper mapper;
    private RawMessageParser parser;

    @BeforeEach
    void setUp() {
        parser = new RawMessageParser(new ObjectMapper());
        mapper = new BusinessEntityMapper(parser);
    }

    @Test
    void toEntityOne_shouldMapSnakeCaseAndDates() {
        var body = parser.parseJson("""
                {"trend_uuid":"tu","trend_name":"name","emotion":"7","is_visible":"1","product_id":"10","group_id":"g","is_archived":"false","employee_id_create":"u","created_at":"2025-01-01T10:15:30Z","prev_product_id":"9"}
                """);

        var data = mapper.toEntityOne(body);

        assertEquals("tu", data.trendUuid());
        assertEquals(7, data.emotion());
        assertTrue(data.isVisible());
        assertNotNull(data.createdAt());
    }

    @Test
    void toEntityTwo_shouldReturnNullsForInvalidDate() {
        var body = parser.parseJson("""
                {"cmId":"cm","trendUuid":"t","summaryUuid":"s","answerDate":"bad-date","clientSegmentCode":"seg"}
                """);

        var data = mapper.toEntityTwo(body);

        assertNull(data.answerDate());
        assertEquals("cm", data.cmId());
    }

    @Test
    void toEntityThree_shouldMapCamelCase() {
        var body = parser.parseJson("""
                {"summaryUuid":"su","summaryName":"sn","trendUuid":"tu","sentiment":"2","isEtalon":"0","employeeIdCreate":"e","createdAt":"2025-02-02T00:00:00Z"}
                """);

        var data = mapper.toEntityThree(body);

        assertEquals("su", data.summaryUuid());
        assertFalse(data.isEtalon());
        assertEquals(2, data.sentiment());
    }
}
