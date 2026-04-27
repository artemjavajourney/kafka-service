package com.example.kafkaservice.apply.support;

import com.example.kafkaservice.apply.ApplyStatusUpdate;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ApplyDedupeUtilTest {

    record Item(String key, long id) {}

    @Test
    void deduplicate_shouldKeepLatestAndMarkOlderAsSkipped() {
        List<Item> items = List.of(new Item("A", 1), new Item("A", 2), new Item("B", 3));
        List<ApplyStatusUpdate> updates = new ArrayList<>();

        var deduped = ApplyDedupeUtil.deduplicate(items, Item::key, Item::id, updates);

        assertEquals(2, deduped.size());
        assertEquals(1, updates.size());
        assertEquals(1, updates.get(0).recordsSkipped());
        assertEquals(1L, updates.get(0).stagingId());
    }
}
