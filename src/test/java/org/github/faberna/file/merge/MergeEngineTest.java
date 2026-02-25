package org.github.faberna.file.merge;

import org.github.faberna.file.segment.model.KeySpec;
import org.github.faberna.file.segment.model.Segment;
import org.github.faberna.file.split.model.Separator;
import org.github.faberna.file.split.model.SingleByteSeparator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.Charset;
import java.util.stream.Stream;

class MergeEngineTest {

    @TempDir
    Path tmp;

    static Stream<Arguments> separators() {
        return Stream.of(
                Arguments.of("LF", new SingleByteSeparator((byte) '\n', 8 * 1024), (byte) '\n'),
                Arguments.of("PIPE", new SingleByteSeparator((byte) '|', 8 * 1024), (byte) '|')
        );
    }

    /*@Test
    void testSplitByKeySpec() throws IOException {
        Path chunk = tmp.resolve("chunk.txt");
        Files.writeString(chunk, "aaaa\nbbbb\ncccc\n");

        KeySpec keySpec = KeySpec.of(Segment.range(0, 4));
        List<String> tokens = SplitEngine.splitByKeySpec(chunk, keySpec, StandardCharsets.UTF_8, new SingleByteSeparator((byte) '\n', 8 * 1024));

        assertEquals(3, tokens.size());
        assertEquals("aaaa", tokens.get(0));
        assertEquals("bbbb", tokens.get(1));
        assertEquals("cccc", tokens.get(2));
    }*/

    @Test
    void testMergeChunks() throws IOException {
        Path c1 = tmp.resolve("c1.txt");
        Path c2 = tmp.resolve("c2.txt");
        Path out = tmp.resolve("out.txt");

        Files.writeString(c1, "aaaa\ncccc\n");
        Files.writeString(c2, "bbbb\ndddd\n");

        KeySpec keySpec = KeySpec.of(Segment.range(0, 4));
        MergeEngine.kWayMerge(List.of(c1, c2), out, keySpec, StandardCharsets.UTF_8, new SingleByteSeparator((byte) '\n', 8 * 1024));

        String outText = Files.readString(out, StandardCharsets.UTF_8);
        String[] tokens = outText.split("\\n");

        assertEquals("aaaa", tokens[0]);
        assertEquals("bbbb", tokens[1]);
        assertEquals("cccc", tokens[2]);
        assertEquals("dddd", tokens[3]);
    }

    private static void writeChunk(Path file, Separator sep, String... records) throws IOException {
        writeChunk(file, sep, true, records);
    }

    private static void writeChunk(Path file, Separator sep, boolean trailingSeparator, String... records) throws IOException {
        try (var out = Files.newOutputStream(file)) {
            for (int i = 0; i < records.length; i++) {
                out.write(records[i].getBytes(StandardCharsets.UTF_8));
                if (trailingSeparator || i < records.length - 1) {
                    out.write(sep.bytes());
                }
            }
        }
    }


    // -------------------------------------------------
    // Additional MergeEngine coverage
    // -------------------------------------------------

    @Test
    void kWayMerge_shouldThrow_whenKeySpecIsNull() {
        assertThrows(IllegalArgumentException.class, () ->
                MergeEngine.kWayMerge(List.of(), tmp.resolve("out.txt"), (KeySpec) null, StandardCharsets.UTF_8, new SingleByteSeparator((byte) '\n', 8 * 1024))
        );
    }

    @Test
    void kWayMerge_shouldThrow_whenComparatorIsNull() {
        assertThrows(IllegalArgumentException.class, () ->
                MergeEngine.kWayMerge(List.of(), tmp.resolve("out.txt"), (Comparator<String>) null, StandardCharsets.UTF_8, new SingleByteSeparator((byte) '\n', 8 * 1024))
        );
    }

    @Test
    void kWayMerge_shouldThrow_whenCharsetIsNull() {
        assertThrows(IllegalArgumentException.class, () ->
                MergeEngine.kWayMerge(List.of(), tmp.resolve("out.txt"), Comparator.naturalOrder(), (Charset) null, new SingleByteSeparator((byte) '\n', 8 * 1024))
        );
    }

    @Test
    void kWayMerge_shouldThrow_whenSeparatorIsNull() {
        assertThrows(IllegalArgumentException.class, () ->
                MergeEngine.kWayMerge(List.of(), tmp.resolve("out.txt"), Comparator.naturalOrder(), StandardCharsets.UTF_8, null)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("separators")
    void kWayMerge_shouldMergeUsingKeySpec_parametrized(String name, Separator sep, byte expectedLastByte) throws IOException {
        KeySpec keySpec = KeySpec.of(Segment.range(0, 4));

        Path c1 = tmp.resolve("p1-" + name + ".txt");
        Path c2 = tmp.resolve("p2-" + name + ".txt");
        Path out = tmp.resolve("out-" + name + ".txt");

        writeChunk(c1, sep, "0001;A", "0004;D");
        writeChunk(c2, sep, "0002;B", "0003;C");

        MergeEngine.kWayMerge(List.of(c1, c2), out, keySpec, StandardCharsets.UTF_8, sep);

        byte[] outBytes = Files.readAllBytes(out);
        assertTrue(outBytes.length > 0);
        assertEquals(expectedLastByte, outBytes[outBytes.length - 1]);

        String outText = new String(outBytes, StandardCharsets.UTF_8);
        String[] tokens = outText.split(name.equals("PIPE") ? "\\|" : "\\n");

        assertEquals("0001;A", tokens[0]);
        assertEquals("0002;B", tokens[1]);
        assertEquals("0003;C", tokens[2]);
        assertEquals("0004;D", tokens[3]);
    }

    @Test
    void kWayMerge_shouldHandleEmptyChunks_gracefully() throws IOException {
        Separator sep = new SingleByteSeparator((byte) '\n', 8 * 1024);
        KeySpec keySpec = KeySpec.of(Segment.range(0, 4));

        Path empty = tmp.resolve("empty.txt");
        Files.write(empty, new byte[0]);

        Path nonEmpty = tmp.resolve("non-empty.txt");
        writeChunk(nonEmpty, sep, "0001;A", "0002;B");

        Path out = tmp.resolve("out-empty-mix.txt");
        MergeEngine.kWayMerge(List.of(empty, nonEmpty), out, keySpec, StandardCharsets.UTF_8, sep);

        String outText = Files.readString(out, StandardCharsets.UTF_8);
        String[] tokens = outText.split("\\n");
        assertEquals("0001;A", tokens[0]);
        assertEquals("0002;B", tokens[1]);
    }

    @Test
    void kWayMerge_shouldMergeSingleChunk_asIs() throws IOException {
        Separator sep = new SingleByteSeparator((byte) '\n', 8 * 1024);
        KeySpec keySpec = KeySpec.of(Segment.range(0, 4));

        Path c1 = tmp.resolve("single.txt");
        writeChunk(c1, sep, "0001;A", "0002;B", "0003;C");

        Path out = tmp.resolve("out-single.txt");
        MergeEngine.kWayMerge(List.of(c1), out, keySpec, StandardCharsets.UTF_8, sep);

        assertArrayEquals(Files.readAllBytes(c1), Files.readAllBytes(out));
    }

    @Test
    void kWayMerge_shouldOutputLastRecordEvenIfChunkMissingTrailingSeparator() throws IOException {
        // This validates that the reader emits the final record at EOF.
        Separator sep = new SingleByteSeparator((byte) '\n', 8 * 1024);
        KeySpec keySpec = KeySpec.of(Segment.range(0, 4));

        Path c1 = tmp.resolve("no-trailing-sep.txt");
        // last record does NOT have separator
        writeChunk(c1, sep, false, "0001;A", "0003;C");

        Path c2 = tmp.resolve("with-trailing-sep.txt");
        writeChunk(c2, sep, "0002;B");

        Path out = tmp.resolve("out-no-trailing.txt");
        MergeEngine.kWayMerge(List.of(c1, c2), out, keySpec, StandardCharsets.UTF_8, sep);

        String outText = Files.readString(out, StandardCharsets.UTF_8);
        // output merge always writes the separator after every record
        String[] tokens = outText.split("\\n");
        assertEquals("0001;A", tokens[0]);
        assertEquals("0002;B", tokens[1]);
        assertEquals("0003;C", tokens[2]);
    }

    @Test
    void kWayMerge_shouldBeStableWhenKeysAreEqual_byUsingSeqTieBreaker() throws IOException {
        // keySpec compares only first 4 chars (the key). All keys below are equal.
        Separator sep = new SingleByteSeparator((byte) '\n', 8 * 1024);
        KeySpec keySpec = KeySpec.of(Segment.range(0, 4));

        Path c1 = tmp.resolve("stable-1.txt");
        Path c2 = tmp.resolve("stable-2.txt");
        Path out = tmp.resolve("out-stable.txt");

        writeChunk(c1, sep, "0001;A1", "0001;A2");
        writeChunk(c2, sep, "0001;B1", "0001;B2");

        MergeEngine.kWayMerge(List.of(c1, c2), out, keySpec, StandardCharsets.UTF_8, sep);

        String outText = Files.readString(out, StandardCharsets.UTF_8);
        String[] tokens = outText.split("\\n");

        // Priming inserts c1 first then c2, so with equal keys the seq tie-break emits c1's first record before c2's.
        assertEquals("0001;A1", tokens[0]);
        assertEquals("0001;B1", tokens[1]);
        assertEquals("0001;A2", tokens[2]);
        assertEquals("0001;B2", tokens[3]);
    }
}

