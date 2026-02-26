package org.github.faberna.file.split.splitter;

import org.github.faberna.file.split.config.IOConfig;
import org.github.faberna.file.split.model.NewlineSeparator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SequentialStreamingSplitterTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldSplitFileByMaxBytes() throws IOException {
        Path input = Path.of("src/test/resources/unsorted.txt");
        Path output = tempDir;
        String prefix = "partSeq-";
        IOConfig ioConfig  = new IOConfig(8*1024*1024,
                0,
                true,
                prefix,
                ".txt");
        SequentialStreamingSplitter splitter = new SequentialStreamingSplitter();
        int maxBytesPerPart = 5 * 1024;
        splitter.splitByMaxBytes(input, output, maxBytesPerPart, new NewlineSeparator(1, null), ioConfig);

        // List generated files
        var parts = Files.list(output)
                .filter(p -> p.getFileName().toString().startsWith(prefix))
                .sorted()
                .toList();

        long inputSize = Files.size(input);
        long totalSize = 0;

        for (int i = 0; i < parts.size(); i++) {
            long size = Files.size(parts.get(i));
            totalSize += size;

            if (i < parts.size() - 1) {
                // All parts except last should be approximately maxBytes
                assertTrue(
                        Math.abs(size - maxBytesPerPart) < 1024,
                        "Part " + (i + 1) + " size not close to expected maxBytes"
                );
            } else {
                // Last part should be <= maxBytes
                assertTrue(size <= maxBytesPerPart, "Last part should not exceed maxBytes");
            }
        }

        // Ensure total size matches original file
        assertEquals(inputSize, totalSize, "Sum of parts must equal original file size");

    }

    @Test
    void splitByMaxBytes_shouldThrow_whenMaxBytesPerPartNonPositive() {
        SequentialStreamingSplitter splitter = new SequentialStreamingSplitter();

        Path input = tempDir.resolve("in.txt");
        Path out = tempDir.resolve("out");

        assertThrows(IllegalArgumentException.class, () ->
                splitter.splitByMaxBytes(input, out, 0L, new NewlineSeparator(1, null), IOConfig.defaults())
        );

        assertThrows(IllegalArgumentException.class, () ->
                splitter.splitByMaxBytes(input, out, -1L, new NewlineSeparator(1, null), IOConfig.defaults())
        );
    }

    @Test
    void splitByMaxBytes_shouldSplitOnNewlineBoundaries_andPreserveBytes() throws Exception {
        SequentialStreamingSplitter splitter = new SequentialStreamingSplitter();

        Path input = tempDir.resolve("in.txt");
        Path outDir = tempDir.resolve("parts");

        // 5 lines. Each line ends with '\n'
        // With maxBytes=6 we expect splits at line boundaries after exceeding target:
        // part1: 2 lines, part2: 2 lines, part3: 1 line  (typical behavior)
        String content = "AAA\nBBB\nCCC\nDDD\nEEE\n";
        Files.writeString(input, content, StandardCharsets.UTF_8);

        IOConfig io = new IOConfig(
                8 * 1024,
                1,
                false,
                "part-",
                ".txt"
        );

        splitter.splitByMaxBytes(input, outDir, 6L, new NewlineSeparator(1, null), io);

        List<Path> parts = listParts(outDir);
        assertEquals(3, parts.size(), "Expected 3 parts (2+2+1 lines)");

        byte[] reconstructed = readAllAndConcat(parts);
        assertArrayEquals(Files.readAllBytes(input), reconstructed);

        // sanity: all parts except last end with '\n'
        for (int i = 0; i < parts.size() - 1; i++) {
            byte[] b = Files.readAllBytes(parts.get(i));
            assertTrue(b.length > 0);
            assertEquals((byte) '\n', b[b.length - 1]);
        }
    }

    @Test
    void splitByMaxBytes_shouldHandleLastRecordWithoutTrailingNewline() throws Exception {
        SequentialStreamingSplitter splitter = new SequentialStreamingSplitter();

        Path input = tempDir.resolve("in.txt");
        Path outDir = tempDir.resolve("parts");

        // last line has NO newline
        String content = "AAA\nBBB\nCCC";
        Files.writeString(input, content, StandardCharsets.UTF_8);

        IOConfig io = new IOConfig(8 * 1024, 1, false, "part-", ".txt");

        splitter.splitByMaxBytes(input, outDir, 5L, new NewlineSeparator(1, null), io);

        List<Path> parts = listParts(outDir);
        assertTrue(parts.size() >= 1);

        byte[] reconstructed = readAllAndConcat(parts);
        assertArrayEquals(Files.readAllBytes(input), reconstructed);
    }

    // -------- helpers --------

    private static List<Path> listParts(Path dir) throws IOException {
        try (var s = Files.list(dir)) {
            return s.filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .toList();
        }
    }

    private static byte[] readAllAndConcat(List<Path> parts) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Path p : parts) {
            out.writeBytes(Files.readAllBytes(p));
        }
        return out.toByteArray();
    }
}