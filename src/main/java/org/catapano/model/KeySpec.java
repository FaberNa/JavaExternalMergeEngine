package org.catapano.model;

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

    /** Allows custom key comparator (materializes key). */
    public Comparator<String> comparator(Comparator<String> keyComparator) {
        if (keyComparator == null) {
            throw new IllegalArgumentException("keyComparator is required");
        }
        return (a, b) -> keyComparator.compare(extractKey(a), extractKey(b));
    }

    /**
     * Compares two lines by their segments, without allocating substrings. Returns first non-zero segment comparison, or 0 if all segments are equal.
     * @param a
     * @param b
     * @return
     */
    private int compareBySegments(String a, String b) {
        for (Segment seg : segment) {
            int c = seg.compare(a, b);
            if (c != 0) return c;
        }
        return 0;
    }

    /**
     * Extracts the full key by concatenating all segments. Used for materialized keys.
     * @param line
     * @return
     */
    public String extractKey(String line) {
        StringBuilder sb = new StringBuilder();
        for (Segment seg : segment) {
            seg.appendKey(line, sb);
        }
        return sb.toString();
    }
}