package org.github.faberna.file.segment.model;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KeySpecComparatorTest {

    // ---------- helpers to instantiate DelimitedSegment without guessing its constructor ----------
    private static Segment delimitedSegment(Object... preferredArgs) {
        try {
            Class<?> cls = Class.forName("org.github.faberna.file.segment.model.DelimitedSegment");

            // 1) Try exact args types first
            for (Constructor<?> c : cls.getDeclaredConstructors()) {
                Class<?>[] pt = c.getParameterTypes();
                if (pt.length != preferredArgs.length) continue;
                if (isCompatible(pt, preferredArgs)) {
                    c.setAccessible(true);
                    return (Segment) c.newInstance(preferredArgs);
                }
            }

            // 2) Try some common fallbacks (you can extend if needed)
            List<Object[]> fallbacks = List.of(
                    new Object[]{';', 0},
                    new Object[]{';', 1},
                    new Object[]{";", 0},
                    new Object[]{";", 1},
                    new Object[]{';'},
                    new Object[]{";"}
            );

            for (Object[] fb : fallbacks) {
                for (Constructor<?> c : cls.getDeclaredConstructors()) {
                    Class<?>[] pt = c.getParameterTypes();
                    if (pt.length != fb.length) continue;
                    if (isCompatible(pt, fb)) {
                        c.setAccessible(true);
                        return (Segment) c.newInstance(fb);
                    }
                }
            }

            fail("Could not instantiate DelimitedSegment via reflection. Please paste DelimitedSegment.java signature.");
            return null;

        } catch (ClassNotFoundException e) {
            fail("DelimitedSegment class not found in package org.github.faberna.file.segment.model");
            return null;
        } catch (Exception e) {
            fail("Failed to instantiate DelimitedSegment via reflection: " + e);
            return null;
        }
    }

    private static boolean isCompatible(Class<?>[] paramTypes, Object[] args) {
        for (int i = 0; i < paramTypes.length; i++) {
            if (args[i] == null) return false;
            Class<?> pt = wrap(paramTypes[i]);
            Class<?> at = wrap(args[i].getClass());
            if (!pt.isAssignableFrom(at)) return false;
        }
        return true;
    }

    private static Class<?> wrap(Class<?> c) {
        if (!c.isPrimitive()) return c;
        if (c == int.class) return Integer.class;
        if (c == long.class) return Long.class;
        if (c == char.class) return Character.class;
        if (c == boolean.class) return Boolean.class;
        if (c == double.class) return Double.class;
        if (c == float.class) return Float.class;
        if (c == byte.class) return Byte.class;
        if (c == short.class) return Short.class;
        return c;
    }

    // ---------- parsing helpers (zero substring/split) ----------
    private static int parseIntRange(String s, int startInclusive, int endExclusive) {
        int v = 0;
        for (int i = startInclusive; i < endExclusive && i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') break;
            v = v * 10 + (c - '0');
        }
        return v;
    }

    private static long parseLongRange(String s, int startInclusive, int endExclusive) {
        long v = 0;
        for (int i = startInclusive; i < endExclusive && i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') break;
            v = v * 10 + (c - '0');
        }
        return v;
    }

    private static int parseIntUntil(String s, char delimiter) {
        int v = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == delimiter) break;
            if (c < '0' || c > '9') continue;
            v = v * 10 + (c - '0');
        }
        return v;
    }

    // ---------- tests ----------

    @Test
    void comparator_shouldUseRangeSegment_lexicographic() {
        // key = first 4 chars (0003, 0100...)
        KeySpec spec = KeySpec.of(new RangeSegment(0, 4));
        Comparator<String> cmp = spec.comparator();

        assertTrue(cmp.compare("0003;foo", "0050;bar") < 0); // "0003" < "0050" lexicographically here
        assertTrue(cmp.compare("0100;foo", "0009;bar") > 0);
        assertEquals(0, cmp.compare("0007;A", "0007;B"));
    }

    @Test
    void comparator_shouldUseDelimitedSegment_lexicographic_onFirstField() {
        // key = first field before ';' (as string)
        Segment seg = Segment.afterDelimiter(';', 0, null); // try with String args

                //delimitedSegment(';', 0); // preferred args (if your constructor matches)
        KeySpec spec = KeySpec.of(seg);
        Comparator<String> cmp = spec.comparator();

        // lexicographic: "10" < "2" because '1' < '2'
        assertTrue(cmp.compare("foo;2", "bar;7") < 0);
        assertEquals(0, cmp.compare("foo;007", "bar;007"));
    }

    @Test
    void comparator_shouldCombineRange_thenDelimited_asSecondaryKey() {
        // Primary: first 4 chars
        // Secondary: second CSV field (index 1) - lexicographic
        Segment primary = new RangeSegment(0, 4);
        Segment secondary = Segment.afterDelimiter(';', 1,null);

        KeySpec spec = KeySpec.of(primary, secondary);
        Comparator<String> cmp = spec.comparator();

        // Same primary, different secondary
        assertTrue(cmp.compare("0003;005;X", "0003;010;Y") < 0);
        assertTrue(cmp.compare("0003;010;Y", "0003;005;X") > 0);

        // Different primary wins regardless of secondary
        assertTrue(cmp.compare("0002;999;X", "0003;000;Y") < 0);
    }

    @Test
    void comparatorFromLine_shouldCompareTypedKey_withoutExtractKey() {
        KeySpec spec = KeySpec.of(new RangeSegment(0, 4)); // segments not used by comparatorFromLine

        Comparator<String> cmp = spec.comparatorFromLine(
                line -> parseIntRange(line, 0, 4),          // typed key = int
                Comparator.naturalOrder()
        );

        // numeric: 3 < 5
        assertTrue(cmp.compare("0003;foo", "0005;bar") < 0);
        assertTrue(cmp.compare("0010;foo", "0002;bar") > 0);
        assertEquals(0, cmp.compare("0007;foo", "0007;bar"));
    }

    @Test
    void comparatorInt_shouldHandle0003_vs_005_numericCorrectly() {
        KeySpec spec = KeySpec.of(new RangeSegment(0, 1)); // just to satisfy KeySpec, not used

        Comparator<String> cmp = spec.comparatorInt(line -> parseIntUntil(line, ';'));

        // This is your case: "0003" vs "005" -> 3 < 5
        assertTrue(cmp.compare("0003;foo", "005;bar") < 0);

        // also: 10 > 2 numerically
        assertTrue(cmp.compare("10;foo", "2;bar") > 0);

        // "7" == "007"
        assertEquals(0, cmp.compare("7;foo", "007;bar"));
    }

    @Test
    void comparatorLong_shouldCompareLargeNumbers_withoutAllocations() {
        KeySpec spec = KeySpec.of(new RangeSegment(0, 1)); // not used

        Comparator<String> cmp = spec.comparatorLong(line -> parseLongRange(line, 0, 12));

        assertTrue(cmp.compare("000000000003;X", "000000000005;Y") < 0);
        assertTrue(cmp.compare("000000000100;X", "000000000009;Y") > 0);
        assertEquals(0, cmp.compare("000000000042;X", "000000000042;Y"));
    }

    @Test
    void comparatorFromLine_shouldThrowOnNulls() {
        KeySpec spec = KeySpec.of(new RangeSegment(0, 1));

        assertThrows(IllegalArgumentException.class,
                () -> spec.comparatorFromLine(null, Comparator.naturalOrder()));

        assertThrows(IllegalArgumentException.class,
                () -> spec.comparatorFromLine(s -> s, null));
    }

    @Test
    void comparatorInt_shouldThrowOnNullExtractor() {
        KeySpec spec = KeySpec.of(new RangeSegment(0, 1));
        assertThrows(IllegalArgumentException.class, () -> spec.comparatorInt(null));
    }

    @Test
    void comparatorLong_shouldThrowOnNullExtractor() {
        KeySpec spec = KeySpec.of(new RangeSegment(0, 1));
        assertThrows(IllegalArgumentException.class, () -> spec.comparatorLong(null));
    }
}