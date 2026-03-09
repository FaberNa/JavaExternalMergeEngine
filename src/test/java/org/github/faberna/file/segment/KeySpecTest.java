package org.github.faberna.file.segment;

import org.apache.poi.ss.formula.functions.T;
import org.github.faberna.file.segment.model.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeySpecTest {

    @Test
    void constructorShouldThrow_whenNullOrEmptySegments() {
        assertThatThrownBy(() -> new KeySpec(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one segment is required");

        assertThatThrownBy(() -> new KeySpec(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At least one segment is required");
    }

    @Test
    void constructorShouldDefensivelyCopyList() {
        var original = new ArrayList<Segment<String>>();
        original.add(new RangeSegment(0,3));

        var spec = new KeySpec<String>(original);

        // mutate original list after creation
        original.add(new RangeSegment(3, 5));

        // KeySpec must not see the new segment (it copied the list)
        assertThat(spec.segment()).hasSize(1);

        // and internal list is unmodifiable (List.copyOf)
        assertThatThrownBy(() -> spec.segment().add(new RangeSegment(1, 2)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void ofFactoryShouldBuildSpecFromVarargs() {
        var s1 = new RangeSegment(0, 2);
        var s2 = new RangeSegment(3, 5);

        var spec = KeySpec.of(s1, s2);

        assertThat(spec.segment()).containsExactly(s1, s2);
    }

    @Test
    void extractKeyShouldConcatenateAllSegmentRanges() {
        var spec = KeySpec.of(
                new RangeSegment(0, 3),  // "ABC"
                new RangeSegment(4, 7)   // "EFG"
        );

        assertThat(spec.extractKey("ABCDEFGH")).isEqualTo("ABCEFG");
    }

    @Test
    void extractKeyShouldHandleShortLinesGracefully() {
        var spec = KeySpec.of(
                new RangeSegment(0, 5),   // up to 5 chars
                new RangeSegment(10, 12)  // start beyond length => ignored
        );

        assertThat(spec.extractKey("ABC")).isEqualTo("ABC");
    }

    @Test
    void defaultComparatorShouldCompareCharByCharAcrossSegments() {
        // Compare first by [0,3), then by [4,7)
        var spec = KeySpec.of(
                new RangeSegment(0, 3),
                new RangeSegment(4, 7)
        );

        var cmp = spec.comparator();

        // first segment differs: "ABC" vs "ABD" -> ABC < ABD
        assertThat(cmp.compare("ABCXEFG", "ABDXEFG")).isLessThan(0);

        // first segment equal, second differs: "EFG" vs "EHG" -> F < H
        assertThat(cmp.compare("ABCXEFG", "ABCXEHG")).isLessThan(0);

        // both equal in compared ranges
        assertThat(cmp.compare("ABCXEFGZZ", "ABCXEFGYY")).isZero();
    }

    @Test
    void defaultComparatorShouldHandleDifferentStringLengths() {
        var spec = KeySpec.of(new RangeSegment(0, 5));
        var cmp = spec.comparator();

        // shorter vs longer within compared range
        assertThat(cmp.compare("ABC", "ABCDE")).isLessThan(0);
        assertThat(cmp.compare("ABCDE", "ABC")).isGreaterThan(0);

        // both shorter and equal
        assertThat(cmp.compare("ABC", "ABC")).isZero();
    }

    @Test
    void comparatorWithCustomKeyComparatorShouldUseExtractKey() {
        var spec = KeySpec.of(
                new RangeSegment(0, 3),  // key part 1
                new RangeSegment(4, 7)   // key part 2
        );

        Comparator<String> reverse = Comparator.reverseOrder();
        var cmp = spec.comparator(reverse);

        // extractKey("ABCXEFG") = "ABCEFG"
        // extractKey("ABDXEFG") = "ABDEFG"
        // reverse order => "ABCEFG" > "ABDEFG" => compare should be > 0
        assertThat(cmp.compare("ABCXEFG", "ABDXEFG")).isGreaterThan(0);

        // sanity check: if keys equal, comparator returns 0
        assertThat(cmp.compare("ABCXEFG", "ABCXEFG")).isZero();
    }


    @Test
    void comparatorWithCustomIntegerComparatorShouldUseSorterByNumber() {
        var spec = KeySpec.of(
                new RangeSegment(0, 6,Mode.INT) // key part 2
        );

        Comparator<String> byIntKey =spec.comparator();
        var cmp = spec.comparator(byIntKey);

        // reverse order => "005" -> "5"  > "0003" -> "3"=> compare should be > 0
        assertThat(cmp.compare("005", "0003")).isGreaterThan(0);

        // sanity check: if keys equal, comparator returns 0
        assertThat(cmp.compare("0025", "000025")).isZero();
    }

    @Test
    void comparatorShouldThrow_whenKeyComparatorIsNull() {
        var spec = KeySpec.of(new RangeSegment(0, 3));

        assertThatThrownBy(() -> spec.comparator((Comparator<String>) null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("keyComparator is required");
    }

    @Test
    void recordBasicsShouldWork() {
        var a = KeySpec.of(new RangeSegment(0, 3));
        var b = KeySpec.of(new RangeSegment(0, 3));
        var c = KeySpec.of(new RangeSegment(0, 2));

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(c);

        assertThat(a.toString()).contains("segment=");
    }

    @Test
    void extractKeyShouldSupportDelimitedSegment() {
        // line: "a|bbb|cc|dddd"
        // occurrenceIndex=1 => 2° delimiter (0-based)
        // after the 2° '|' there is "cc|dddd" -> first 3 chars => "cc|"
        var spec = KeySpec.of(new DelimitedSegment('|', 1, 3, Mode.LEX));

        assertThat(spec.extractKey("a|bbb|cc|dddd")).isEqualTo("cc|");
    }

    @Test
    void compareShouldSupportDelimitedSegmentInIntMode() {
        DelimitedSegment segment = new DelimitedSegment('|', 0, null, Mode.INT);

        assertThat(segment.compare("A|2|x", "A|10|x")).isLessThan(0);
        assertThat(segment.compare("A|10|x", "A|2|x")).isGreaterThan(0);
        assertThat(segment.compare("A|002|x", "A|2|x")).isZero();
    }

    @Test
    void compareShouldSupportDelimitedSegmentInFloatMode() {
        DelimitedSegment segment = new DelimitedSegment('|', 0, null, Mode.FLOAT);

        assertThat(segment.compare("A|2.5|x", "A|10.25|x")).isLessThan(0);
        assertThat(segment.compare("A|10.25|x", "A|2.5|x")).isGreaterThan(0);
        assertThat(segment.compare("A|2.50|x", "A|2.5|x")).isZero();
    }

    @Test
    void defaultComparatorShouldSupportDelimitedSegment() {
        // Compare by first 2 chars after the first delimiter
        var spec = KeySpec.of(new DelimitedSegment('|', 0, 2, Mode.LEX));
        var cmp = spec.comparator();

        // "a|ba|x" -> key "ba"
        // "a|bb|x" -> key "bb"
        assertThat(cmp.compare("a|ba|x", "a|bb|x")).isLessThan(0);

        // same key => 0 even if the rest differs
        assertThat(cmp.compare("a|bb|x", "z|bb|y")).isZero();
    }

    @Test
    void comparatorShouldWorkWithMixedSegments() {
        // First compare the first char (range), then 2 chars after the first delimiter
        var spec = KeySpec.of(
                new RangeSegment(0, 1),
                new DelimitedSegment('|', 0, 2, Mode.LEX)
        );

        var cmp = spec.comparator();

        // first char differs
        assertThat(cmp.compare("a|bb|x", "b|aa|x")).isLessThan(0);

        // first char equal, compare part after delimiter: "bb" vs "bc"
        assertThat(cmp.compare("a|bb|x", "a|bc|x")).isLessThan(0);
    }
}