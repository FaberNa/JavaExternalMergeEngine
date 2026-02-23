package org.catapano.file.segment.model;

import org.catapano.file.segment.util.SegmentUtil;

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

        /** Compares the specified range of two lines char by char, without allocating substrings. */
        @Override
        public int compare(String a, String b) {
            return SegmentUtil.compareRangesCharByChar(a, start, end, b, start, end);
        }

        @Override
        public void appendKey(String line, StringBuilder out) {
            SegmentUtil.appendRange(line, start, end, out);
        }
    }

