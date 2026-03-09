package org.github.faberna.file.segment.model;

public record RangeSegment(int start, int end, Mode mode)implements Segment<String> {


    public RangeSegment(int start, int end) {
        this(start, end, Mode.LEX);
    }

    public RangeSegment {
        if (start < 0 || end <= start)  throw new IllegalArgumentException("Invalid inclusive range: " + start + " to " + end);

        if (mode == null) throw new IllegalArgumentException("mode is required");
    }

    @Override
    public int compare(String a, String b) {
        return switch (mode) {
            case LEX -> compareLex(a, b);
            case INT -> Long.compare(parseLongInRange(a, start, end), parseLongInRange(b, start, end));
            case FLOAT -> Double.compare(parseDoubleInRange(a, start, end), parseDoubleInRange(b, start, end));
        };
    }

    private int compareLex(String a, String b) {
        for (int i = start; i < end; i++) {
            char ca = (i < a.length()) ? a.charAt(i) : 0;
            char cb = (i < b.length()) ? b.charAt(i) : 0;
            if (ca != cb) return Character.compare(ca, cb);
        }
        return 0;
    }

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

    @Override
    public void appendKey(String line, StringBuilder out) {
        int to = Math.min(end, line.length());
        if (start >= to) return;
        out.append(line, start, to);
    }

    public int length() {
        return end - start;
    }
}