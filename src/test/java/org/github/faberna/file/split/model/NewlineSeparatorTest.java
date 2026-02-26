package org.github.faberna.file.split.model;

import org.github.faberna.file.split.util.FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class NewlineSeparatorTest {

    @TempDir
    Path tempDir;

    @ParameterizedTest(name = "{index} input={0} expectedEnd={1}")
    @MethodSource("newlineCases")
    void shouldFindNewlineAndReturnEndExclusive(String content, long expectedEndExclusive) throws Exception {
        Path file = writeBytes(content.getBytes(StandardCharsets.US_ASCII));
        long fileSize = Files.size(file);

        NewlineSeparator sep = new NewlineSeparator(8, null);
        try (FileChannel ch = FileChannel.open(file)) {
            long end = sep.findNextSeparatorEnd(ch, 0, fileSize);
            assertEquals(expectedEndExclusive, end);
        }
    }

    @ParameterizedTest(name = "{index} input={0} expectedSeparator={1}")
    @MethodSource("newlineCases2")
    void shouldFindNewlineSeparatorOnRealFile(String content, String expectedSeparator) throws Exception {
        Path file = writeBytes(content.getBytes(StandardCharsets.US_ASCII));

        String separator = FileUtil.recognizeNewLineSeparatorForFile(file);
        assertEquals(expectedSeparator, separator);
    }



    private static Stream<Arguments> newlineCases() {
        return Stream.of(
                Arguments.of("AAA\nBBB", 4L),
                Arguments.of("AAA\r\nBBB", 5L),
                Arguments.of("AAA\rBBB", 4L)
        );
    }

    private static Stream<Arguments> newlineCases2() {
        return Stream.of(
                Arguments.of("AAA\nBBB", "\n"),
                Arguments.of("AAA\r\nBBB", "\r\n"),
                Arguments.of("AAA\rBBB", "\r")
        );
    }

    @Test
    void shouldRejectNonPositiveBufferSize() {
        assertThrows(IllegalArgumentException.class, () -> new NewlineSeparator(0, null));
        assertThrows(IllegalArgumentException.class, () -> new NewlineSeparator(-1, null));
    }

    @Test
    void shouldHandleCrLfSpanningBuffers() throws Exception {
        // Force CR at end of a read buffer and LF at start of next buffer.
        // Content: "ABCD\r\nEF" -> CR at index 4, LF at 5, endExclusive should be 6
        byte[] bytes = "ABCD\r\nEF".getBytes(StandardCharsets.US_ASCII);
        Path file = writeBytes(bytes);
        long fileSize = Files.size(file);

        // bufferSize=5 ensures first read can end exactly on CR boundary in many implementations
        NewlineSeparator sep = new NewlineSeparator(5, null);
        try (FileChannel ch = FileChannel.open(file)) {
            long end = sep.findNextSeparatorEnd(ch, 0, fileSize);
            assertEquals(6L, end);
        }
    }

    @Test
    void shouldReturnFMinusOneWhenNoNewlineFound() throws Exception {
        // No newline at all -> return fileSize (meaning boundary at EOF)
        Path file = writeBytes("ABCDEF".getBytes(StandardCharsets.US_ASCII));
        long fileSize = Files.size(file);

        NewlineSeparator sep = new NewlineSeparator(8, null);
        try (FileChannel ch = FileChannel.open(file)) {
            long end = sep.findNextSeparatorEnd(ch, 0, fileSize);
            assertEquals(-1L, end);
        }
    }

    @Test
    void shouldFindNextSeparatorStartingFromOffset() throws Exception {
        // "A\nB\nC" -> from offset 2 should find the second newline (at index 3) => endExclusive 4
        Path file = writeBytes("A\nB\nC".getBytes(StandardCharsets.US_ASCII));
        long fileSize = Files.size(file);

        NewlineSeparator sep = new NewlineSeparator(8, null);
        try (FileChannel ch = FileChannel.open(file)) {
            long end = sep.findNextSeparatorEnd(ch, 2, fileSize);
            assertEquals(4L, end);
        }
    }

    private Path writeBytes(byte[] bytes) throws IOException {
        Path p = tempDir.resolve("in.txt");
        Files.write(p, bytes);
        return p;
    }


    @Test
    void shouldHandleLoneCrSpanningBuffersAndReturnPos() throws Exception {
        // Force CR at end of a read buffer and a non-LF byte at start of next buffer.
        // Content: "ABCD\rXEF" -> CR at index 4, next byte is 'X' (not '\n').
        // When CR is last byte in first buffer, pendingCR=true; next buffer starts at pos=5.
        // Expected boundary is endExclusive after the lone CR, which is 5 (the pos of the next buffer).
        byte[] bytes = "ABCD\rXEF".getBytes(StandardCharsets.US_ASCII);
        Path file = writeBytes(bytes);
        long fileSize = Files.size(file);

        // bufferSize=5 makes first read likely end exactly at the CR boundary
        NewlineSeparator sep = new NewlineSeparator(5, null);
        try (FileChannel ch = FileChannel.open(file)) {
            long end = sep.findNextSeparatorEnd(ch, 0, fileSize);
            assertEquals(5L, end);
        }
    }

    @Test
    void shouldTreatNegativeFromAsZero() throws Exception {
        // from < 0 is normalized to 0
        Path file = writeBytes("AAA\nBBB".getBytes(StandardCharsets.US_ASCII));
        long fileSize = Files.size(file);

        NewlineSeparator sep = new NewlineSeparator(8, null);
        try (FileChannel ch = FileChannel.open(file)) {
            long end = sep.findNextSeparatorEnd(ch, -123, fileSize);
            assertEquals(4L, end); // same as from=0
        }
    }

    @Test
    void shouldReturnMinusOneWhenFromIsAtOrBeyondFileSize() throws Exception {
        Path file = writeBytes("AAA\nBBB".getBytes(StandardCharsets.US_ASCII));
        long fileSize = Files.size(file);

        NewlineSeparator sep = new NewlineSeparator(8, null);
        try (FileChannel ch = FileChannel.open(file)) {
            assertEquals(-1L, sep.findNextSeparatorEnd(ch, fileSize, fileSize));
            assertEquals(-1L, sep.findNextSeparatorEnd(ch, fileSize + 10, fileSize));
        }
    }

}