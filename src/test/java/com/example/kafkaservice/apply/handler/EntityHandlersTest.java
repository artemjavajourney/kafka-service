package com.example.kafkaservice.apply.handler;

import com.example.kafkaservice.apply.ApplyCandidate;
import com.example.kafkaservice.apply.ApplyStatusUpdate;
import com.example.kafkaservice.apply.BusinessEntityMapper;
import com.example.kafkaservice.apply.model.EntityOneData;
import com.example.kafkaservice.apply.model.EntityThreeData;
import com.example.kafkaservice.apply.model.EntityTwoData;
import com.example.kafkaservice.apply.model.EntityTwoKey;
import com.example.kafkaservice.finaltable.repository.EntityOneRepository;
import com.example.kafkaservice.finaltable.repository.EntityThreeRepository;
import com.example.kafkaservice.finaltable.repository.EntityTwoRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

class EntityHandlersTest {

    private final ObjectMapper om = new ObjectMapper();

    @Test
    void entityOneHandler_shouldMarkFailedWithoutKey() throws Exception {
        BusinessEntityMapper mapper = mock(BusinessEntityMapper.class);
        EntityOneRepository repo = mock(EntityOneRepository.class);
        when(mapper.toEntityOne(any())).thenReturn(new EntityOneData(null, null, null, null, null, null, null, null, null, null));

        EntityOneApplyHandler handler = new EntityOneApplyHandler(mapper, repo);
        List<ApplyStatusUpdate> updates = new ArrayList<>();
        handler.handle(List.of(new ApplyCandidate(1L, "L", "ENTITY_1", om.readTree("{}"))), updates);

        assertEquals(1, updates.size());
        verify(repo, never()).batchUpsert(anyList());
    }

    @Test
    void entityTwoHandler_shouldDeferWhenKeyIncomplete() throws Exception {
        BusinessEntityMapper mapper = mock(BusinessEntityMapper.class);
        EntityTwoRepository repo = mock(EntityTwoRepository.class);
        when(mapper.toEntityTwo(any())).thenReturn(new EntityTwoData(null, null, null, null, null, null, null, null, null));

        EntityTwoApplyHandler handler = new EntityTwoApplyHandler(mapper, repo);
        List<ApplyStatusUpdate> updates = new ArrayList<>();
        handler.handle(List.of(new ApplyCandidate(2L, "L", "ENTITY_2", om.readTree("{}"))), updates);

        assertEquals(1, updates.size());
        assertEquals("DEFERRED", updates.get(0).status().name());
    }

    @Test
    void entityThreeHandler_shouldInsertWhenMissingInDatabase() throws Exception {
        BusinessEntityMapper mapper = mock(BusinessEntityMapper.class);
        EntityThreeRepository repo = mock(EntityThreeRepository.class);
        EntityThreeData data = new EntityThreeData("s1", "n", "t", 1, true, "u", null);
        when(mapper.toEntityThree(any())).thenReturn(data);
        when(repo.findComparableBySummaryUuids(any())).thenReturn(Map.of());

        EntityThreeApplyHandler handler = new EntityThreeApplyHandler(mapper, repo);
        List<ApplyStatusUpdate> updates = new ArrayList<>();
        handler.handle(List.of(new ApplyCandidate(3L, "L", "ENTITY_3", om.readTree("{}"))), updates);

        verify(repo).batchUpsert(anyList());
        assertEquals(1, updates.size());
        assertEquals("APPLIED", updates.get(0).status().name());
    }

    @Test
    void entityTwoHandler_shouldSkipWhenNoChanges() throws Exception {
        BusinessEntityMapper mapper = mock(BusinessEntityMapper.class);
        EntityTwoRepository repo = mock(EntityTwoRepository.class);
        EntityTwoData data = new EntityTwoData("cm", LocalDate.now(), null, 1, "tr", "su", "seg", 1, 2);
        when(mapper.toEntityTwo(any())).thenReturn(data);
        when(repo.findComparableByKeys(any())).thenReturn(Map.of(
                data.key(), new EntityTwoRepository.EntityTwoComparable(1, "seg", 1, 2)
        ));

        EntityTwoApplyHandler handler = new EntityTwoApplyHandler(mapper, repo);
        List<ApplyStatusUpdate> updates = new ArrayList<>();
        handler.handle(List.of(new ApplyCandidate(4L, "L", "ENTITY_2", om.readTree("{}"))), updates);

        verify(repo).batchUpsert(anyList());
        assertEquals("SKIPPED", updates.get(0).status().name());
    }
}
