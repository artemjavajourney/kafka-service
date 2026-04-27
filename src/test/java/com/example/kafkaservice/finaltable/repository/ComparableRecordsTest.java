package com.example.kafkaservice.finaltable.repository;

import com.example.kafkaservice.apply.model.EntityOneData;
import com.example.kafkaservice.apply.model.EntityThreeData;
import com.example.kafkaservice.apply.model.EntityTwoData;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComparableRecordsTest {

    @Test
    void entityOneComparable_shouldDetectChanges() {
        var comparable = new EntityOneRepository.EntityOneComparable("n", 1, true, 2, "g", false, "u", 3);
        var unchanged = new EntityOneData("id", null, null, null, null, null, null, null, null, null);
        var changed = new EntityOneData("id", "n2", null, null, null, null, null, null, null, null);

        assertFalse(comparable.isChangedComparedTo(unchanged));
        assertTrue(comparable.isChangedComparedTo(changed));
    }

    @Test
    void entityTwoComparable_shouldDetectChanges() {
        var comparable = new EntityTwoRepository.EntityTwoComparable(1, "seg", 10, 9);
        var unchanged = new EntityTwoData("cm", LocalDate.now(), null, null, null, null, null, null, null);
        var changed = new EntityTwoData("cm", LocalDate.now(), null, 2, null, null, null, null, null);

        assertFalse(comparable.isChangedComparedTo(unchanged));
        assertTrue(comparable.isChangedComparedTo(changed));
    }

    @Test
    void entityThreeComparable_shouldDetectChanges() {
        var comparable = new EntityThreeRepository.EntityThreeComparable("n", "tr", 1, true, "u");
        var unchanged = new EntityThreeData("id", null, null, null, null, null, null);
        var changed = new EntityThreeData("id", "new", null, null, null, null, null);

        assertFalse(comparable.isChangedComparedTo(unchanged));
        assertTrue(comparable.isChangedComparedTo(changed));
    }
}
