package com.example.kafkaservice.apply;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class ApplyDedupeUtil {

    private ApplyDedupeUtil() {
    }

    public static <K, T> Map<K, T> deduplicate(
            List<T> items,
            Function<T, K> keyExtractor,
            Function<T, Long> stagingIdExtractor,
            List<ApplyStatusUpdate> statusUpdates
    ) {
        Map<K, T> latest = new LinkedHashMap<>();
        for (T item : items) {
            latest.put(keyExtractor.apply(item), item);
        }

        Set<Long> latestIds = latest.values().stream().map(stagingIdExtractor).collect(Collectors.toSet());
        items.stream()
                .map(stagingIdExtractor)
                .filter(id -> !latestIds.contains(id))
                .forEach(id -> statusUpdates.add(ApplyStatusUpdate.skipped(id, "Obsolete version in batch")));

        return latest;
    }
}
