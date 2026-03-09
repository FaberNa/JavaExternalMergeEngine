package org.github.faberna.file.segment.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RangeSegmentCompareTest {

    @Test
    void compareShouldUseLexModeWhenModeIsLex() {
        RangeSegment segment = new RangeSegment(0, 3, Mode.LEX);

        assertThat(segment.compare("abcXXX", "abdYYY")).isLessThan(0);
        assertThat(segment.compare("abdYYY", "abcXXX")).isGreaterThan(0);
        assertThat(segment.compare("abc111", "abc222")).isZero();
    }

    @Test
    void compareLexShouldConsiderOnlyConfiguredRange() {
        RangeSegment segment = new RangeSegment(2, 5, Mode.LEX);

        assertThat(segment.compare("xxabcZZ", "yyabdTT")).isLessThan(0);
        assertThat(segment.compare("xxabdZZ", "yyabcTT")).isGreaterThan(0);
        assertThat(segment.compare("11abcAA", "22abcBB")).isZero();
    }

    @Test
    void compareLexShouldHandleShortStringsUsingZeroCharPadding() {
        RangeSegment segment = new RangeSegment(0, 4, Mode.LEX);

        assertThat(segment.compare("ab", "abz")).isLessThan(0);
        assertThat(segment.compare("abz", "ab")).isGreaterThan(0);
        assertThat(segment.compare("ab", "ab")).isZero();
    }

    @Test
    void compareShouldUseIntModeWhenModeIsInt() {
        RangeSegment segment = new RangeSegment(0, 4, Mode.INT);

        assertThat(segment.compare("0010rest", "0002rest")).isGreaterThan(0);
        assertThat(segment.compare("0002rest", "0010rest")).isLessThan(0);
        assertThat(segment.compare("0007rest", "0007xxxx")).isZero();
    }

    @Test
    void compareIntShouldIgnoreLeadingSpacesAndReadSign() {
        RangeSegment segment = new RangeSegment(0, 5, Mode.INT);

        assertThat(segment.compare("  +7abc", "   2abc")).isGreaterThan(0);
        assertThat(segment.compare("  -7abc", "   2abc")).isLessThan(0);
        assertThat(segment.compare("  +7abc", "00007xx")).isZero();
    }

    @Test
    void compareIntShouldReturnZeroWhenNoDigitsAreFound() {
        RangeSegment segment = new RangeSegment(0, 4, Mode.INT);

        assertThat(segment.compare("abcd", "zzzz")).isZero();
        assertThat(segment.compare("abcd", "0001")).isLessThan(0);
        assertThat(segment.compare("0001", "abcd")).isGreaterThan(0);
    }

    @Test
    void compareIntShouldStopParsingAtFirstNonDigit() {
        RangeSegment segment = new RangeSegment(0, 5, Mode.INT);

        assertThat(segment.compare("12abc", "13xyz")).isLessThan(0);
        assertThat(segment.compare("15abc", "13xyz")).isGreaterThan(0);
        assertThat(segment.compare("12abc", "12zzz")).isZero();
    }

    @Test
    void compareShouldUseFloatModeWhenModeIsFloat() {
        RangeSegment segment = new RangeSegment(0, 6, Mode.FLOAT);

        assertThat(segment.compare("002.50rest", "010.25rest")).isLessThan(0);
        assertThat(segment.compare("010.25rest", "002.50rest")).isGreaterThan(0);
        assertThat(segment.compare("002.50rest", "002.50xxxx")).isZero();
    }

    @Test
    void compareFloatShouldIgnoreLeadingSpacesAndReadSign() {
        RangeSegment segment = new RangeSegment(0, 6, Mode.FLOAT);

        assertThat(segment.compare(" +2.5xx", "  2.4yy")).isGreaterThan(0);
        assertThat(segment.compare(" -2.5xx", "  2.4yy")).isLessThan(0);
        assertThat(segment.compare(" +2.5xx", "002.5yy")).isZero();
    }

    @Test
    void compareFloatShouldHandleIntegerPartWithoutFraction() {
        RangeSegment segment = new RangeSegment(0, 4, Mode.FLOAT);

        assertThat(segment.compare("0012x", "0011y")).isGreaterThan(0);
        assertThat(segment.compare("0011x", "0012y")).isLessThan(0);
        assertThat(segment.compare("0012x", "0012y")).isZero();
    }

    @Test
    void compareFloatShouldReturnZeroWhenNoDigitsAreFound() {
        RangeSegment segment = new RangeSegment(0, 5, Mode.FLOAT);

        assertThat(segment.compare("abcde", "zzzzz")).isZero();
        assertThat(segment.compare("abcde", "1.25x")).isLessThan(0);
        assertThat(segment.compare("1.25x", "abcde")).isGreaterThan(0);
    }

    @Test
    void compareFloatShouldStopParsingAtFirstInvalidCharacterAfterFraction() {
        RangeSegment segment = new RangeSegment(0, 6, Mode.FLOAT);

        assertThat(segment.compare("1.2abc", "1.3xyz")).isLessThan(0);
        assertThat(segment.compare("1.4abc", "1.3xyz")).isGreaterThan(0);
        assertThat(segment.compare("1.2abc", "1.20zz")).isZero();
    }
}