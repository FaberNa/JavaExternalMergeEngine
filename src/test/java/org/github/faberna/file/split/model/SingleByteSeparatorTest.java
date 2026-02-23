package org.github.faberna.file.split.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class SingleByteSeparatorTest {

    @TempDir
    Path tempDir;

    // ==========================
    // Constructor validation
    // ==========================

    @ParameterizedTest
    @ValueSource(ints = {0, -1, -100})
    void shouldRejectNonPositiveBufferSize(int bufferSize) {
        assertThrows(IllegalArgumentException.class,
                () -> new SingleByteSeparator((byte) ';', bufferSize));
    }

    @Test
    void shouldReturnConfiguredSeparatorByte() {
        SingleByteSeparator sep = new SingleByteSeparator((byte) '%', 8);
        assertEquals((byte) '%', sep.getSep());
    }

    // ==========================
    // Guard clauses
    // ==========================

    @Test
    void shouldThrowWhenChannelIsNull() {
        SingleByteSeparator sep = new SingleByteSeparator((byte) ';', 8);
        assertThrows(NullPointerException.class,
                () -> sep.findNextSeparatorEnd(null, 0, 10));
    }

    @Test
    void shouldTreatNegativeFromAsZero() throws Exception {
        Path file = writeBytes("AAA;BBB".getBytes(StandardCharsets.US_ASCII));
        long fileSize = Files.size(file);

        SingleByteSeparator sep = new SingleByteSeparator((byte) ';', 8);
        try (FileChannel ch = FileChannel.open(file)) {
            long end = sep.findNextSeparatorEnd(ch, -123, fileSize);
            assertEquals(4L, end); // ';' at index 3 -> endExclusive 4
        }
    }

    @Test
    void shouldReturnMinusOneWhenFromIsAtOrBeyondFileSize() throws Exception {
        Path file = writeBytes("A;B".getBytes(StandardCharsets.US_ASCII));
        long fileSize = Files.size(file);

        SingleByteSeparator sep = new SingleByteSeparator((byte) ';', 8);
        try (FileChannel ch = FileChannel.open(file)) {
            assertEquals(-1L, sep.findNextSeparatorEnd(ch, fileSize, fileSize));
            assertEquals(-1L, sep.findNextSeparatorEnd(ch, fileSize + 10, fileSize));
        }
    }

    // ==========================
    // Found cases
    // ==========================

    @ParameterizedTest(name = "{index} content=''{0}'' from={1} expectedEnd={2}")
    @MethodSource("foundCases")
    void shouldFindSeparatorAndReturnEndExclusive(
            String content,
            long from,
            long expectedEndExclusive
    ) throws Exception {

        Path file = writeBytes(content.getBytes(StandardCharsets.US_ASCII));
        long fileSize = Files.size(file);

        SingleByteSeparator sep = new SingleByteSeparator((byte) ';', 8);

        try (FileChannel ch = FileChannel.open(file)) {
            long end = sep.findNextSeparatorEnd(ch, from, fileSize);
            assertEquals(expectedEndExclusive, end);
        }
    }

    private static Stream<Arguments> foundCases() {
        return Stream.of(
                Arguments.of(";ABC", 0L, 1L),       // at beginning
                Arguments.of("ABC;DEF", 0L, 4L),    // in middle
                Arguments.of("ABCDEF;", 0L, 7L),    // at end
                Arguments.of("A;B;C", 0L, 2L),      // first occurrence
                Arguments.of("A;B;C", 2L, 4L)       // search from offset -> second occurrence
        );
    }

    // ==========================
    // Not found cases
    // ==========================

    @ParameterizedTest(name = "{index} content=''{0}'' from={1}")
    @MethodSource("notFoundCases")
    void shouldReturnMinusOneWhenSeparatorNotFound(String content, long from) throws Exception {

        Path file = writeBytes(content.getBytes(StandardCharsets.US_ASCII));
        long fileSize = Files.size(file);

        SingleByteSeparator sep = new SingleByteSeparator((byte) ';', 8);

        try (FileChannel ch = FileChannel.open(file)) {
            long end = sep.findNextSeparatorEnd(ch, from, fileSize);
            assertEquals(-1L, end);
        }
    }

    private static Stream<Arguments> notFoundCases() {
        return Stream.of(
                Arguments.of("ABCDEF", 0L),
                Arguments.of("ABCDEF", 3L),
                Arguments.of(";ABC", 1L) // from after the only separator
        );
    }

    // ==========================
    // Buffer boundary case
    // ==========================

    @Test
    void shouldWorkWithBufferSizeOneAcrossMultipleReads() throws Exception {
        // separator at index 5
        Path file = writeBytes("ABCDE;FGHIJ".getBytes(StandardCharsets.US_ASCII));
        long fileSize = Files.size(file);

        SingleByteSeparator sep = new SingleByteSeparator((byte) ';', 1);

        try (FileChannel ch = FileChannel.open(file)) {
            long end = sep.findNextSeparatorEnd(ch, 0, fileSize);
            assertEquals(6L, end); // ';' at 5 -> endExclusive 6
        }
    }

    // ==========================
    // Utility
    // ==========================

    private Path writeBytes(byte[] bytes) throws IOException {
        Path p = tempDir.resolve("input.bin");
        Files.write(p, bytes);
        return p;
    }
}