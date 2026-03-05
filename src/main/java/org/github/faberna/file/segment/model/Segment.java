package org.github.faberna.file.segment.model;

public sealed interface Segment permits RangeSegment, DelimitedSegment {

    int compare(String a, String b);
    void appendKey(String line, StringBuilder out);

    static Segment range(int startInclusive, int endExclusive) {
        return new RangeSegment(startInclusive, endExclusive, Mode.LEX);
    }

    static Segment rangeInt(int startInclusive, int endExclusive) {
        return new RangeSegment(startInclusive, endExclusive, Mode.INT);
    }

    static Segment rangeFloat(int startInclusive, int endExclusive) {
        return new RangeSegment(startInclusive, endExclusive, Mode.FLOAT);
    }

    static Segment afterDelimiter(char delimiter, int occurrenceIndex, Integer lengthAfter) {
        return new DelimitedSegment(delimiter, occurrenceIndex, lengthAfter, Mode.LEX);
    }

    static Segment afterDelimiterInt(char delimiter, int occurrenceIndex, Integer lengthAfter) {
        return new DelimitedSegment(delimiter, occurrenceIndex, lengthAfter, Mode.INT);
    }

    static Segment afterDelimiterFloat(char delimiter, int occurrenceIndex, Integer lengthAfter) {
        return new DelimitedSegment(delimiter, occurrenceIndex, lengthAfter, Mode.FLOAT);
    }
}