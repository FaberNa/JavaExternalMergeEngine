package org.github.faberna.file.segment.model;


public sealed interface Segment permits RangeSegment, DelimitedSegment {
    /** Compare this segment between two lines without allocating substrings. */
    int compare(String a, String b);

    /** Append this segment's key portion (for debug / materialized key). */
    void appendKey(String line, StringBuilder out);

    // Factory helpers (optional, nice to have)
    static Segment range(int startInclusive, int endExclusive) {
        return new RangeSegment(startInclusive, endExclusive);
    }

    /**
     * Extracts a segment after the N-th occurrence of a delimiter, with a fixed length.
     * @param delimiter
     * @param occurrenceIndex
     * @param lengthAfter
     * @return
     */
    static Segment afterDelimiter(char delimiter, int occurrenceIndex, Integer lengthAfter) {
        return new DelimitedSegment(delimiter, occurrenceIndex, lengthAfter);
    }// per extractKey()
}