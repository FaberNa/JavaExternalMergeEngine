package org.github.faberna.file.merge;

import org.github.faberna.file.segment.model.KeySpec;
import org.github.faberna.file.segment.model.RangeSegment;
import org.github.faberna.file.segment.model.Segment;
import org.github.faberna.file.split.model.NewlineSeparator;
import org.github.faberna.file.split.model.Separator;
import org.github.faberna.file.split.model.SingleByteSeparator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MergeEngineKeySpecSeparatorTest {

    @TempDir
    Path tmp;

    @Test
    void kWayMerge_shouldMergeUsingKeySpecAndNewLineSeparator() throws IOException {
        // Given: custom separator (NOT newline)
        Separator sep = new NewlineSeparator(8);
        Path tmp = Path.of("src/test/resources/output");
        // And: KeySpec based on fixed-width numeric-ish key in first 4 chars (lexicographic works for 0001,0002,...)
        KeySpec keySpec = KeySpec.of( Segment.range(0, 4));

        // Two already-sorted chunks using the SAME separator
        Path c1 = tmp.resolve("chunk-1-k1.txt");
        Path c2 = tmp.resolve("chunk-2-k1.txt");
        Path out = tmp.resolve("out-k1.txt");

        // chunk1: 0001, 0004
        writeChunk(c1, sep, "0001;A", "0004;D");

        // chunk2: 0002, 0003
        writeChunk(c2, sep, "0002;B", "0003;C");

        // When
        MergeEngine.kWayMerge(List.of(c1, c2), out, keySpec, StandardCharsets.UTF_8, sep);

        // Then: output is globally sorted and separator preserved
        byte[] outBytes = Files.readAllBytes(out);

        // must end with separator
        assertTrue(outBytes.length > 0, "output should not be empty");
        assertEquals((byte) '\n', outBytes[outBytes.length - 1], "output should end with record separator");

        String outText = new String(outBytes, StandardCharsets.UTF_8);

        // split by separator and drop trailing empty token caused by last separator
        String[] tokens = outText.split("\\\n");
        // because string ends with '|', split() returns last token empty -> ignore it
        assertEquals("0001;A", tokens[0]);
        assertEquals("0002;B", tokens[1]);
        assertEquals("0003;C", tokens[2]);
        assertEquals("0004;D", tokens[3]);
    }

    @Test
    void kWayMerge_shouldMergeUsingKeySpecAndCustomSeparator() throws IOException {
        // Given: custom separator (NOT newline)
        Separator sep = new SingleByteSeparator((byte) '|', 8 * 1024);

        // And: KeySpec based on fixed-width numeric-ish key in first 4 chars (lexicographic works for 0001,0002,...)
        KeySpec keySpec = KeySpec.of(new RangeSegment(0, 4));

        // Two already-sorted chunks using the SAME separator
        Path c1 = tmp.resolve("chunk-1.txt");
        Path c2 = tmp.resolve("chunk-2.txt");
        Path out = tmp.resolve("out.txt");

        // chunk1: 0001, 0004
        writeChunk(c1, sep, "0001;A", "0004;D");

        // chunk2: 0002, 0003
        writeChunk(c2, sep, "0002;B", "0003;C");

        // When
        MergeEngine.kWayMerge(List.of(c1, c2), out, keySpec, StandardCharsets.UTF_8, sep);

        // Then: output is globally sorted and separator preserved
        byte[] outBytes = Files.readAllBytes(out);

        // must end with separator
        assertTrue(outBytes.length > 0, "output should not be empty");
        assertEquals((byte) '|', outBytes[outBytes.length - 1], "output should end with record separator");

        String outText = new String(outBytes, StandardCharsets.UTF_8);

        // split by separator and drop trailing empty token caused by last separator
        String[] tokens = outText.split("\\|");
        // because string ends with '|', split() returns last token empty -> ignore it
        assertEquals("0001;A", tokens[0]);
        assertEquals("0002;B", tokens[1]);
        assertEquals("0003;C", tokens[2]);
        assertEquals("0004;D", tokens[3]);
    }


    @Test
    void kWayMerge_shouldMergeRealFileUsingKeySpecAndNewLineSeparator() throws IOException {
        // Given: custom separator (NOT newline)
        Separator sep = new NewlineSeparator(8);
        Path tmp = Path.of("src/test/resources");
        // And: KeySpec based on fixed-width numeric-ish key in first 4 chars (lexicographic works for 0001,0002,...)
        KeySpec keySpec = KeySpec.of( Segment.range(0, 4));

        // Two already-sorted chunks using the SAME separator
        Path c1 = tmp.resolve("unsorted.txt");
        Path c2 = tmp.resolve("chunk-2-k1.txt");
        Path out = tmp.resolve("out-k1.txt");

        // chunk1: 0001, 0004
        writeChunk(c1, sep, "0001;A", "0004;D");

        // chunk2: 0002, 0003
        writeChunk(c2, sep, "0002;B", "0003;C");

        // When
        MergeEngine.kWayMerge(List.of(c1, c2), out, keySpec, StandardCharsets.UTF_8, sep);

        // Then: output is globally sorted and separator preserved
        byte[] outBytes = Files.readAllBytes(out);

        // must end with separator
        assertTrue(outBytes.length > 0, "output should not be empty");
        assertEquals((byte) '\n', outBytes[outBytes.length - 1], "output should end with record separator");

        String outText = new String(outBytes, StandardCharsets.UTF_8);

        // split by separator and drop trailing empty token caused by last separator
        String[] tokens = outText.split("\\\n");
        // because string ends with '|', split() returns last token empty -> ignore it
        assertEquals("0001;A", tokens[0]);
        assertEquals("0002;B", tokens[1]);
        assertEquals("0003;C", tokens[2]);
        assertEquals("0004;D", tokens[3]);
    }

    private static void writeChunk(Path file, Separator sep, String... records) throws IOException {
        // write raw bytes: record + separator.bytes()
        try (var out = Files.newOutputStream(file)) {
            for (String r : records) {
                out.write(r.getBytes(StandardCharsets.UTF_8));
                out.write(sep.bytes());
            }
        }
    }
}