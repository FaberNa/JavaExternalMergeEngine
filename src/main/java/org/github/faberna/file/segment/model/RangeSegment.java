package org.github.faberna.file.segment.model;

import org.github.faberna.file.segment.util.SegmentUtil;

/**
 * Positional segment of a key.
 * Internal representation: 0-based start (inclusive), end (exclusive).
 */
public record RangeSegment(int start, int end) implements Segment {

        public RangeSegment {
            if (start < 0 || end <= start) {
                throw new IllegalArgumentException("Invalid segment range: start=" + start + ", end=" + end);
            }
        }

        /** 1-based inclusive positions -> internal 0-based [start,end) */
        public static RangeSegment ofInclusive(int start1Based, int end1Based) {
            if (start1Based <= 0 || end1Based < start1Based) {
                throw new IllegalArgumentException("Invalid 1-based inclusive range: " + start1Based + " to " + end1Based);
            }
            return new RangeSegment(start1Based - 1, end1Based);
        }

        public int length() { return end - start; }

    @Override
    public int compare(String a, String b) {
        // Compare [start, end) treating missing chars as '\0'
        for (int i = start; i < end; i++) {
            char ca = (i < a.length()) ? a.charAt(i) : 0;
            char cb = (i < b.length()) ? b.charAt(i) : 0;
            if (ca != cb) {
                return Character.compare(ca, cb);
            }
        }
        return 0;
    }

    @Override
    public void appendKey(String line, StringBuilder out) {
        int to = Math.min(end, line.length());
        if (start >= to) return;
        out.append(line, start, to);
    }
    }

