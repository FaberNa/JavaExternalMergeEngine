package org.catapano.model;

import org.catapano.util.SegmentUtil;

/**
 * Segment defined by a delimiter and its occurrence index.
 * Optionally, a fixed length after the delimiter can be specified; if not, it extends
 * @param delimiter
 * @param occurrenceIndex
 * @param lengthAfter
 */
public record DelimitedSegment(char delimiter, int occurrenceIndex, Integer lengthAfter) implements Segment {

    public DelimitedSegment {
        if (occurrenceIndex < 0) {
            throw new IllegalArgumentException("occurrenceIndex must be >= 0");
        }
        if (lengthAfter != null && lengthAfter <= 0) {
            throw new IllegalArgumentException("lengthAfter must be > 0 when specified");
        }
    }

    @Override
    public int compare(String a, String b) {
        Range ra = resolve(a);
        Range rb = resolve(b);
        return SegmentUtil.compareRangesCharByChar(a, ra.start, ra.end, b, rb.start, rb.end);
    }

    @Override
    public void appendKey(String line, StringBuilder out) {
        Range r = resolve(line);
        SegmentUtil.appendRange(line, r.start, r.end, out);
    }

    private Range resolve(String line) {
        int delimPos = SegmentUtil.nthDelimiterIndex(line, delimiter, occurrenceIndex);
        if (delimPos < 0) {
            // delimitatore non trovato => segmento vuoto
            return new Range(line.length(), line.length());
        }

        int start = delimPos + 1;

        if (lengthAfter == null) {
            int nextDelim = SegmentUtil.nthDelimiterIndex(line, delimiter, occurrenceIndex + 1);
            int end = (nextDelim < 0) ? line.length() : nextDelim;
            return new Range(start, end);
        }

        int end = Math.min(line.length(), start + lengthAfter);
        return new Range(start, end);
    }

    private record Range(int start, int end) {}
}