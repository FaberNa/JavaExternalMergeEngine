package org.catapano.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class RangeSegmentTest {
    @Test
    void shouldValidateConstructorArguments() {
        // start < 0
        assertThatThrownBy(() -> Segment.range(-1, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid segment range");

        // end <= start
        assertThatThrownBy(() -> Segment.range(5, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid segment range");

        assertThatThrownBy(() -> Segment.range(10, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid segment range");
    }

    @Test
    void ofInclusiveShouldConvert1BasedToZeroBasedCorrectly() {
        // 1-based inclusive: 1..3 => internal 0..3
        RangeSegment seg = RangeSegment.ofInclusive(1, 3);

        assertThat(seg.start()).isEqualTo(0);
        assertThat(seg.end()).isEqualTo(3);
        assertThat(seg.length()).isEqualTo(3);
    }

    @Test
    void ofInclusiveShouldValidateInput() {
        assertThatThrownBy(() -> RangeSegment.ofInclusive(0, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid 1-based inclusive range");

        assertThatThrownBy(() -> RangeSegment.ofInclusive(5, 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid 1-based inclusive range");
    }

    @Test
    void lengthShouldReturnCorrectSize() {
        RangeSegment seg = (RangeSegment) Segment.range(2, 7);
        assertThat(seg.length()).isEqualTo(5);
    }

    @Test
    void compareShouldWorkCharByChar() {
        Segment seg = Segment.range(0, 3);

        // Compare first 3 characters
        assertThat(seg.compare("ABCDEF", "ABDXYZ"))
                .isLessThan(0);  // C < D

        assertThat(seg.compare("ABDDEF", "ABCXYZ"))
                .isGreaterThan(0); // D > C

        assertThat(seg.compare("ABCxxx", "ABCyyy"))
                .isZero(); // first 3 chars equal
    }

    @Test
    void compareShouldHandleShorterStrings() {
        RangeSegment seg = new RangeSegment(0, 5);

        // left shorter
        assertThat(seg.compare("ABC", "ABCDE"))
                .isLessThan(0);

        // right shorter
        assertThat(seg.compare("ABCDE", "ABC"))
                .isGreaterThan(0);

        // both shorter but equal
        assertThat(seg.compare("ABC", "ABC"))
                .isZero();
    }

    @Test
    void appendKeyShouldAppendCorrectRange() {
        Segment seg = Segment.range(1, 4);

        StringBuilder sb = new StringBuilder();
        seg.appendKey("ABCDE", sb);

        assertThat(sb.toString()).isEqualTo("BCD");
    }

    @Test
    void appendKeyShouldHandleShortLineGracefully() {
        Segment seg = Segment.range(2,6);

        StringBuilder sb = new StringBuilder();
        seg.appendKey("ABC", sb);  // only index 2 exists

        assertThat(sb.toString()).isEqualTo("C");
    }

    @Test
    void recordBasicsShouldWork() {
        Segment a = Segment.range(0, 5);
        Segment b = Segment.range(0, 5);
        Segment c = Segment.range(1, 5);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(c);

        assertThat(a.toString())
                .contains("start=")
                .contains("end=");
    }

}