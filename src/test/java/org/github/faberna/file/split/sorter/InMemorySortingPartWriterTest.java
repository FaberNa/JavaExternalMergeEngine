package org.github.faberna.file.split.sorter;

import org.github.faberna.file.segment.model.KeySpec;
import org.github.faberna.file.segment.model.Segment;
import org.github.faberna.file.split.model.LineEnding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class InMemorySortingPartWriterTest {

    @TempDir
    Path tempDir;

    static Stream<Charset> charsets() {
        return Stream.of(StandardCharsets.UTF_8, StandardCharsets.US_ASCII);
    }

    // ✅ TODO: adatta questa funzione al tuo KeySpec reale
    private static KeySpec keySpecWholeLine() {
        // ESEMPIO A (se hai un PositionalSegment):
        // return new KeySpec(List.of(new PositionalSegment(0, Integer.MAX_VALUE)));

        // ESEMPIO B (se hai factory):
         return KeySpec.of(Segment.range(0, Integer.MAX_VALUE));

        //throw new UnsupportedOperationException("Implement keySpecWholeLine() using your real KeySpec/Segment API");
    }

    @Test
    void shouldRejectNullConstructorArgs() {
        KeySpec ks = keySpecWholeLine();
        Comparator<String> kc = Comparator.naturalOrder();
        Charset cs = StandardCharsets.UTF_8;

        assertThrows(NullPointerException.class, () -> new InMemorySortingPartWriter(null, kc, cs));
        assertThrows(NullPointerException.class, () -> new InMemorySortingPartWriter(ks, null, cs));
        assertThrows(NullPointerException.class, () -> new InMemorySortingPartWriter(ks, kc, null));
    }

    @ParameterizedTest
    @MethodSource("charsets")
    void shouldReturnCharsetProvidedInConstructor(Charset charset) {
        InMemorySortingPartWriter w = new InMemorySortingPartWriter(keySpecWholeLine(), Comparator.naturalOrder(), charset);
        assertEquals(charset, w.getCharset());
    }

    @Test
    void endPartShouldRejectNullPath() {
        InMemorySortingPartWriter w = new InMemorySortingPartWriter(keySpecWholeLine(), Comparator.naturalOrder(), StandardCharsets.UTF_8);
        assertThrows(NullPointerException.class, () -> w.endPart(null));
    }

    @ParameterizedTest
    @MethodSource("charsets")
    void shouldSortLinesAndFixNoneInMiddle_AndMakeLastNoTerminator(Charset charset) throws Exception {
        InMemorySortingPartWriter writer = new InMemorySortingPartWriter(keySpecWholeLine(), Comparator.naturalOrder(), charset);

        writer.acceptLine("b", LineEnding.NONE);
        writer.acceptLine("c", LineEnding.LF);
        writer.acceptLine("a", LineEnding.LF);

        Path out = tempDir.resolve("part-0001.txt");
        writer.endPart(out);

        String expected =
                "a" + LineEnding.LF.text() +
                        "b" + LineEnding.LF.text() +   // NONE era in mezzo => diventa defaultEnding
                        "c" + LineEnding.NONE.text();  // ultimo defaultEnding => diventa NONE

        assertEquals(expected, Files.readString(out, charset));
    }

    @ParameterizedTest
    @MethodSource("charsets")
    void shouldHandleAllNoneEndings_DefaultToLfAndLeaveLastWithoutTerminator(Charset charset) throws Exception {
        InMemorySortingPartWriter writer = new InMemorySortingPartWriter(keySpecWholeLine(), Comparator.naturalOrder(), charset);

        writer.acceptLine("c", LineEnding.NONE);
        writer.acceptLine("a", LineEnding.NONE);
        writer.acceptLine("b", LineEnding.NONE);

        Path out = tempDir.resolve("part-0002.txt");
        writer.endPart(out);

        String expected =
                "a" + LineEnding.LF.text() +
                        "b" + LineEnding.LF.text() +
                        "c" + LineEnding.NONE.text();

        assertEquals(expected, Files.readString(out, charset));
    }

    @ParameterizedTest
    @MethodSource("charsets")
    void shouldClearBufferAfterEndPart(Charset charset) throws Exception {
        InMemorySortingPartWriter writer = new InMemorySortingPartWriter(keySpecWholeLine(), Comparator.naturalOrder(), charset);

        Path out1 = tempDir.resolve("part-0003.txt");
        writer.acceptLine("b", LineEnding.LF);
        writer.acceptLine("a", LineEnding.LF);
        writer.endPart(out1);

        assertEquals("a" + LineEnding.LF.text() + "b" + LineEnding.NONE.text(), Files.readString(out1, charset));

        Path out2 = tempDir.resolve("part-0004.txt");
        writer.endPart(out2);

        assertEquals("", Files.readString(out2, charset));
    }
}