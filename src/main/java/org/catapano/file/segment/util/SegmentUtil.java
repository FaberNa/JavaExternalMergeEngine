package org.catapano.file.segment.util;

public final class SegmentUtil {
    private SegmentUtil() {}

    public static int compareRangesCharByChar(String a, int aStart, int aEnd, String b, int bStart, int bEnd) {
        int aLen = a.length();
        int bLen = b.length();

        int aStop = Math.min(aEnd, aLen);
        int bStop = Math.min(bEnd, bLen);

        int ai = Math.max(0, aStart);
        int bi = Math.max(0, bStart);

        while (ai < aStop || bi < bStop) {
            char ca = ai < aStop ? a.charAt(ai) : 0;
            char cb = bi < bStop ? b.charAt(bi) : 0;
            if (ca != cb) return Character.compare(ca, cb);
            ai++; bi++;
        }
        return 0;
    }

    /**
     * Appends the substring of `line` from `start` (inclusive) to `end` (exclusive) to `out`, handling out-of-bounds gracefully.
     * @param line
     * @param start
     * @param end
     * @param out
     */
    public static void appendRange(String line, int start, int end, StringBuilder out) {
        int len = line.length();
        if (start >= len) return;
        int s = Math.max(0, start);
        int e = Math.min(end, len);
        if (s < e) out.append(line, s, e);
    }

    /**
     *   Returns the index of the N-th occurrence of `delimiter` in `line`, or -1if not found.
     * @param line
     * @param delimiter
     * @param n
     * @return
     */
    public static int nthDelimiterIndex(String line, char delimiter, int n) {
        int count = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == delimiter) {
                if (count == n) return i;
                count++;
            }
        }
        return -1;
    }

}
