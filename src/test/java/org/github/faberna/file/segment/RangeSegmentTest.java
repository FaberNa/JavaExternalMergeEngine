package org.github.faberna.file.segment;

import org.github.faberna.file.segment.model.RangeSegment;
import org.github.faberna.file.segment.model.Segment;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class RangeSegmentTest {
    @Test
    void shouldValidateConstructorArguments() {
        // start < 0
        assertThatThrownBy(() -> new RangeSegment(-1, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid inclusive range: -1 to 5");

        // end <= start
        assertThatThrownBy(() -> new RangeSegment(5, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid inclusive range: 5 to 5");

        assertThatThrownBy(() -> new RangeSegment(10, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid inclusive range: 10 to 5");
    }

    @Test
    void ofInclusiveShouldConvert1BasedToZeroBasedCorrectly() {
        // 1-based inclusive: 1..3 => internal 0..3
        RangeSegment seg = new RangeSegment(0, 3);

        assertThat(seg.start()).isZero();
        assertThat(seg.end()).isEqualTo(3);
        assertThat(seg.length()).isEqualTo(3);
    }

    @Test
    void ofInclusiveShouldValidateInput() {
        assertThatThrownBy(() -> new RangeSegment(-1, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid inclusive range: -1 to 5");

        assertThatThrownBy(() -> new RangeSegment(5, 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid inclusive range: 5 to 4");
    }

    @Test
    void lengthShouldReturnCorrectSize() {
        RangeSegment seg =  new RangeSegment(2, 7);
        assertThat(seg.length()).isEqualTo(5);
    }

    @Test
    void compareShouldWorkCharByChar() {
        Segment<String> seg = new RangeSegment(0, 3);

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
        Segment<String> seg = new RangeSegment(1, 4);

        StringBuilder sb = new StringBuilder();
        seg.appendKey("ABCDE", sb);

        assertThat(sb.toString()).hasToString("BCD");
    }

    @Test
    void appendKeyShouldHandleShortLineGracefully() {
        Segment<String> seg = new RangeSegment(2,6);

        StringBuilder sb = new StringBuilder();
        seg.appendKey("ABC", sb);  // only index 2 exists

        assertThat(sb.toString()).hasToString("C");
    }

    @Test
    void recordBasicsShouldWork() {
        Segment<String> a = new RangeSegment(0, 5);
        Segment<String> b = new RangeSegment(0, 5);
        Segment<String> c = new RangeSegment(1, 5);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).hasSameHashCodeAs(b.hashCode());
        assertThat(a).isNotEqualTo(c);

        assertThat(a.toString())
                .contains("start=")
                .contains("end=");
    }

}