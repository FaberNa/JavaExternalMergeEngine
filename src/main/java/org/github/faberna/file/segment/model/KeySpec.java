package org.github.faberna.file.segment.model;

import org.apache.poi.ss.formula.functions.T;

import java.util.Comparator;
import java.util.List;

/** * Describes how to extract a key from a line, via one or more segments.
 * Each segment is a substring of the line, defined by an offset and length.
 * The key is the concatenation of all segments.
 */
public record KeySpec<T>(List<Segment<T>> segment) {

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
    public Comparator<T> comparator() {
        return this::compareBySegments;
    }



    /**
     * Materializes the key via {@link #extractKey(T)} (allocates),
     * then compares using the provided comparator.
     */
    public Comparator<T> comparator(Comparator<String> keyComparator) {
        if (keyComparator == null) throw new IllegalArgumentException("keyComparator is required");
        return (a, b) -> keyComparator.compare(extractKey(a), extractKey(b));
    }


    private int compareBySegments(T a, T b) {
        for (Segment<T> seg : segment) {
            int c = seg.compare(a, b);
            if (c != 0) return c;
        }
        return 0;
    }

    /** Allocates. Use only for debugging / materialized-key comparators. */
    public String extractKey(T line) {
        StringBuilder sb = new StringBuilder();
        for (Segment<T> seg : segment) seg.appendKey(line, sb);
        return sb.toString();
    }
}