package org.github.faberna.file.segment.model;

import java.util.Comparator;
import java.util.List;

public record KeySpec(List<Segment> segment) {

    public KeySpec {
        if (segment == null || segment.isEmpty()) {
            throw new IllegalArgumentException("At least one segment is required");
        }
        segment = List.copyOf(segment);
    }

    public static KeySpec of(Segment... segments) {
        return new KeySpec(List.of(segments));
    }

    /** Default zero-allocation comparator (delegates to segments). */
    public Comparator<String> comparator() {
        return this::compareBySegments;
    }

    /**
     * Materializes the key via {@link #extractKey(String)} (allocates),
     * then compares using the provided comparator.
     */
    public Comparator<String> comparator(Comparator<String> keyComparator) {
        if (keyComparator == null) throw new IllegalArgumentException("keyComparator is required");
        return (a, b) -> keyComparator.compare(extractKey(a), extractKey(b));
    }

    public <K> Comparator<String> comparator(java.util.function.Function<String, K> keyMapper,
                                             Comparator<? super K> keyComparator) {
        if (keyMapper == null) throw new IllegalArgumentException("keyMapper is required");
        if (keyComparator == null) throw new IllegalArgumentException("keyComparator is required");
        return (a, b) -> keyComparator.compare(keyMapper.apply(extractKey(a)),
                keyMapper.apply(extractKey(b)));
    }

    public <K> Comparator<String> comparatorFromLine(java.util.function.Function<String, K> keyExtractor,
                                                     Comparator<? super K> keyComparator) {
        if (keyExtractor == null) throw new IllegalArgumentException("keyExtractor is required");
        if (keyComparator == null) throw new IllegalArgumentException("keyComparator is required");
        return (a, b) -> keyComparator.compare(keyExtractor.apply(a), keyExtractor.apply(b));
    }

    public Comparator<String> comparatorLong(java.util.function.ToLongFunction<String> keyExtractor) {
        if (keyExtractor == null) throw new IllegalArgumentException("keyExtractor is required");
        return Comparator.comparingLong(keyExtractor);
    }

    public Comparator<String> comparatorInt(java.util.function.ToIntFunction<String> keyExtractor) {
        if (keyExtractor == null) throw new IllegalArgumentException("keyExtractor is required");
        return Comparator.comparingInt(keyExtractor);
    }

    private int compareBySegments(String a, String b) {
        for (Segment seg : segment) {
            int c = seg.compare(a, b);
            if (c != 0) return c;
        }
        return 0;
    }

    /** Allocates. Use only for debugging / materialized-key comparators. */
    public String extractKey(String line) {
        StringBuilder sb = new StringBuilder();
        for (Segment seg : segment) seg.appendKey(line, sb);
        return sb.toString();
    }
}