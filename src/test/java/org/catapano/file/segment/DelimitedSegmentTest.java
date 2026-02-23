package org.catapano.file.segment;

import org.catapano.file.segment.model.DelimitedSegment;
import org.catapano.file.segment.model.Segment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DelimitedSegmentTest {
    @Test
    void shouldValidateConstructorArgs() {
        // occurrenceIndex must be >= 0
        assertThatThrownBy(() -> new DelimitedSegment('|', -1, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("occurrenceIndex");

        // lengthAfter must be > 0
        assertThatThrownBy(() -> new DelimitedSegment('|', 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lengthAfter");

        assertThatThrownBy(() -> new DelimitedSegment('|', 0, -5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lengthAfter");
    }

    @ParameterizedTest(name = "lengthAfter=null: \"{0}\" -> \"{1}\"")
    @CsvSource({
            // nextDelim >= 0  => end = nextDelim (si ferma al prossimo delimiter)
            "A;B;C;D, B",
            "A;BLAST;C;D, BLAST",

            // nextDelim < 0  => end = line.length() (fino a fine riga)
            "A;B, B",

            // delimPos < 0 => segmento vuoto (delimiter non trovato)
            "ABC, ''",

            // consecutive delimiters => substring vuota tra ;; (nextDelim immediato)
            "A;;C, ''"
    })
    void shouldExtractUntilNextDelimiterOrEol_WhenLengthAfterIsNull(String line, String expected) {
        var segment = new DelimitedSegment(';', 0, null);

        var out = new StringBuilder();
        segment.appendKey(line, out);

        assertEquals(expected, out.toString());
    }

    @Test
    void shouldAppendExpectedKeyPortion_afterNthDelimiter() {
        // ABC|123|HELLO|999
        // after 1st '|' (occurrence 0) take 3 => "123"
        var seg0 = new DelimitedSegment('|', 0, 3);
        var sb0 = new StringBuilder();
        seg0.appendKey("ABC|123|HELLO|999", sb0);
        assertThat(sb0.toString()).isEqualTo("123");

        // after 2nd '|' (occurrence 1) take 3 => "HEL"
        var seg1 = new DelimitedSegment('|', 1, 3);
        var sb1 = new StringBuilder();
        seg1.appendKey("ABC|123|HELLO|999", sb1);
        assertThat(sb1.toString()).isEqualTo("HEL");

        // after 3rd '|' (occurrence 2) take 3 => "999"
        var seg2 = new DelimitedSegment('|', 2, 3);
        var sb2 = new StringBuilder();
        seg2.appendKey("ABC|123|HELLO|999", sb2);
        assertThat(sb2.toString()).isEqualTo("999");
    }

    @Test
    void shouldAppendEmpty_whenDelimiterOccurrenceDoesNotExist() {
        var seg = new DelimitedSegment('|', 5, 3); // 6th delimiter, doesn't exist
        var sb = new StringBuilder("prefix-");
        seg.appendKey("ABC|123|HELLO|999", sb);

        // No changes expected if segment resolves to empty range
        assertThat(sb.toString()).isEqualTo("prefix-");
    }

    @Test
    void shouldAppendUpToLineEnd_whenNotEnoughCharsAfterDelimiter() {
        // line ends shortly after delimiter
        var seg = new DelimitedSegment('|', 0, 10);
        var sb = new StringBuilder();
        seg.appendKey("A|BC", sb); // after '|' there are only 2 chars: "BC"
        assertThat(sb.toString()).isEqualTo("BC");
    }

    @Test
    void compareShouldOrderLexicographically_onExtractedPortion() {
        var seg = new DelimitedSegment('|', 0, 3);

        // compare extracted: "123" vs "124"
        assertThat(seg.compare("X|123|Z", "X|124|Z")).isLessThan(0);

        // compare extracted: "200" vs "100"
        assertThat(seg.compare("X|200|Z", "X|100|Z")).isGreaterThan(0);

        // equal extracted portion
        assertThat(seg.compare("X|123|A", "Y|123|B")).isZero();
    }

    @Test
    void compareShouldTreatMissingDelimiterAsEmptySegment() {
        var seg = new DelimitedSegment('|', 0, 3);

        // left missing => empty, right has "123" => empty < "123"
        assertThat(seg.compare("ABC", "ABC|123")).isLessThan(0);

        // both missing => equal
        assertThat(seg.compare("ABC", "DEF")).isZero();

        // right missing => "123" > empty
        assertThat(seg.compare("ABC|123", "ABC")).isGreaterThan(0);
    }

    @Test
    void recordBasics_equalsHashCodeToStringShouldWork() {
        var a = new DelimitedSegment('|', 1, 3);
        var b = new DelimitedSegment('|', 1, 3);
        var c = new DelimitedSegment('|', 2, 3);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(c);

        // toString contains components (record default)
        assertThat(a.toString()).contains("delimiter=", "occurrenceIndex=", "lengthAfter=");
    }

    @Test
    void shouldWorkAlsoThroughFactory_ifYouUseSegmentAfterDelimiterFactory() {
        Segment seg = Segment.afterDelimiter('|', 1, 3);
        var sb = new StringBuilder();
        seg.appendKey("ABC|123|HELLO|999", sb);
        assertThat(sb.toString()).isEqualTo("HEL");
    }

    @ParameterizedTest(name = "extract lengthAfter=null from \"{0}\" => \"{1}\"")
    @CsvSource({
            "A;B;C;D, B",
            "A;BLAST;C;D, BLAST",
            "ABC, ''"
    })
    void shouldExtractExpectedPortion_WhenLengthAfterIsNull(String line, String expected) {
        DelimitedSegment segment = new DelimitedSegment(';', 0, null);

        StringBuilder out = new StringBuilder();
        segment.appendKey(line, out);

        assertEquals(expected, out.toString());
    }

    @Test
    void shouldCompareCorrectly_WhenLengthAfterIsNull() {
        DelimitedSegment segment = new DelimitedSegment(';', 0, null);

        String a = "A;B;C";
        String b = "A;C;C";

        int result = segment.compare(a, b);

        assertTrue(result < 0);
    }

}