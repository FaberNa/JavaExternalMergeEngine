package org.github.faberna.file.segment.model;

import org.github.faberna.file.segment.util.SegmentUtil;

public record DelimitedSegment(char delimiter, int occurrenceIndex, Integer lengthAfter, Mode mode) implements Segment {


    public DelimitedSegment(char delimiter, int occurrenceIndex, Integer lengthAfter) {
        this(delimiter, occurrenceIndex, lengthAfter, Mode.LEX);
    }

    public DelimitedSegment {
        if (occurrenceIndex < 0) throw new IllegalArgumentException("occurrenceIndex must be >= 0");
        if (lengthAfter != null && lengthAfter <= 0) throw new IllegalArgumentException("lengthAfter must be > 0");
        if (mode == null) throw new IllegalArgumentException("mode is required");
    }

    @Override
    public int compare(String a, String b) {
        Range ra = resolve(a);
        Range rb = resolve(b);

        return switch (mode) {
            case LEX -> SegmentUtil.compareRangesCharByChar(a, ra.start, ra.end, b, rb.start, rb.end);
            case INT -> Long.compare(parseLongInRange(a, ra.start, ra.end), parseLongInRange(b, rb.start, rb.end));
            case FLOAT -> Double.compare(parseDoubleInRange(a, ra.start, ra.end), parseDoubleInRange(b, rb.start, rb.end));
        };
    }

    @Override
    public void appendKey(String line, StringBuilder out) {
        Range r = resolve(line);
        SegmentUtil.appendRange(line, r.start, r.end, out);
    }

    private Range resolve(String line) {
        int delimPos = SegmentUtil.nthDelimiterIndex(line, delimiter, occurrenceIndex);
        if (delimPos < 0) return new Range(line.length(), line.length());

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

    // stessi parser in-place di RangeSegment (copiati qui per non dipendere da SegmentUtil)
    private static long parseLongInRange(String s, int start, int end) {
        int n = s.length();
        int i = Math.min(start, n);
        int to = Math.min(end, n);
        while (i < to && s.charAt(i) <= ' ') i++;

        boolean neg = false;
        if (i < to) {
            char c = s.charAt(i);
            if (c == '-' || c == '+') { neg = (c == '-'); i++; }
        }

        long val = 0;
        boolean any = false;
        while (i < to) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') break;
            any = true;
            val = val * 10 + (c - '0');
            i++;
        }
        if (!any) return 0;
        return neg ? -val : val;
    }

    private static double parseDoubleInRange(String s, int start, int end) {
        int n = s.length();
        int i = Math.min(start, n);
        int to = Math.min(end, n);
        while (i < to && s.charAt(i) <= ' ') i++;

        boolean neg = false;
        if (i < to) {
            char c = s.charAt(i);
            if (c == '-' || c == '+') { neg = (c == '-'); i++; }
        }

        double val = 0.0;
        boolean any = false;
        while (i < to) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') break;
            any = true;
            val = val * 10.0 + (c - '0');
            i++;
        }

        if (i < to && s.charAt(i) == '.') {
            i++;
            double div = 10.0;
            while (i < to) {
                char c = s.charAt(i);
                if (c < '0' || c > '9') break;
                any = true;
                val += (c - '0') / div;
                div *= 10.0;
                i++;
            }
        }

        if (!any) return 0.0;
        return neg ? -val : val;
    }
}