package org.github.faberna.file.split;

import static org.junit.jupiter.api.Assertions.*;


import org.github.faberna.file.segment.model.KeySpec;
import org.github.faberna.file.segment.model.RangeSegment;
import org.github.faberna.file.segment.model.Segment;
import org.github.faberna.file.split.config.IOConfig;
import org.github.faberna.file.split.model.NewlineSeparator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

class SplitEngineTest {

    @TempDir
    Path tempDir;

    @Test
    @EnabledIfSystemProperty(named = "run.large.tests", matches = "true")
    void shouldSplitFileByMaxBytes() throws Exception {

        Path input = Path.of("src/test/resources/test_1gb.txt");
        Path outputDir = tempDir;
        //Path outputDir = Path.of("src/test/resources/output");;

        SplitEngine engine = new SplitEngine();

        IOConfig io = new IOConfig(
                8 * 1024 * 1024,   // copy buffer
                4,                 // parallelism
                false,             // preferSequential
                "part-",
                ".txt"
        );

        long maxBytes = 100L * 1024 * 1024; // 100MB per part

        engine.splitByMaxBytes(
                input,
                outputDir,
                maxBytes,
                new NewlineSeparator(io.copyBufferBytes()),
                io
        );

        // ---- Assertions ----

        // List generated files
        var parts = Files.list(outputDir)
                .filter(p -> p.getFileName().toString().startsWith("part-"))
                .sorted()
                .toList();

        long inputSize = Files.size(input);

        long expectedParts = inputSize / maxBytes;
        if (inputSize % maxBytes != 0) {
            expectedParts++; // last partial part
        }

        assertEquals(expectedParts, parts.size(),
                "Number of generated parts does not match expected division");


        long totalSize = 0;

        for (int i = 0; i < parts.size(); i++) {
            long size = Files.size(parts.get(i));
            totalSize += size;

            if (i < parts.size() - 1) {
                // All parts except last should be approximately maxBytes
                assertTrue(
                        Math.abs(size - maxBytes) < 1024,
                        "Part " + (i + 1) + " size not close to expected maxBytes"
                );
            } else {
                // Last part should be <= maxBytes
                assertTrue(size <= maxBytes, "Last part should not exceed maxBytes");
            }
        }

        // Ensure total size matches original file
        assertEquals(inputSize, totalSize, "Sum of parts must equal original file size");


    }

    @Test
    void shouldSplitAndSortParallelFileByMaxBytes() throws Exception {

        Path input = Path.of("src/test/resources/unsorted.txt");
        Path outputDir = tempDir;
       // Path outputDir = Path.of("src/test/resources/output");;

        Segment segment = new RangeSegment(0,10);
        KeySpec keySpec = new KeySpec(List.of(segment)); // sort by first 10 chars of each line

        SortedSplitEngine engine = new SortedSplitEngine(new SplitEngine(), keySpec, keySpec.comparator());

        IOConfig io = new IOConfig(
                8 * 1024 * 1024,   // copy buffer
                2,                 // parallelism
                false,             // preferSequential
                "sorted-",
                ".txt"
        );

        long maxBytes = 100L * 1024 * 1024; // 100MB per part

        engine.splitByMaxBytesSorted(
                input,
                outputDir,
                maxBytes,
                new NewlineSeparator(io.copyBufferBytes()),
                io,
                StandardCharsets.UTF_8
        );

        // ---- Assertions ----

        // List generated files
        var parts = Files.list(outputDir)
                .filter(p -> p.getFileName().toString().startsWith("sorted-"))
                .sorted()
                .toList();

        long inputSize = Files.size(input);

        long expectedParts = inputSize / maxBytes;
        if (inputSize % maxBytes != 0) {
            expectedParts++; // last partial part
        }

        assertEquals(expectedParts, parts.size(),
                "Number of generated parts does not match expected division");


        long totalSize = 0;

        for (int i = 0; i < parts.size(); i++) {
            long size = Files.size(parts.get(i));
            totalSize += size;

            if (i < parts.size() - 1) {
                // All parts except last should be approximately maxBytes
                assertTrue(
                        Math.abs(size - maxBytes) < 1024,
                        "Part " + (i + 1) + " size not close to expected maxBytes"
                );
            } else {
                // Last part should be <= maxBytes
                assertTrue(size <= maxBytes, "Last part should not exceed maxBytes");
            }
        }

        // Ensure total size matches original file
        assertEquals(inputSize, totalSize, "Sum of parts must equal original file size");


    }

    @Test
    void shouldSplitAndSortSequentialFileByMaxBytes() throws Exception {

        Path input = Path.of("src/test/resources/unsorted.txt");
        Path outputDir = tempDir;
        //Path outputDir = Path.of("src/test/resources/output");;

        Segment segment = new RangeSegment(0,10);
        KeySpec keySpec = new KeySpec(List.of(segment)); // sort by first 10 chars of each line

        SortedSplitEngine engine = new SortedSplitEngine(new SplitEngine(), keySpec, keySpec.comparator());

        IOConfig io = new IOConfig(
                8 * 1024 * 1024,   // copy buffer
                2,                 // parallelism
                true,             // preferSequential
                "sortedSeq-",
                ".txt"
        );

        long maxBytes = 100L * 1024 * 1024; // 100MB per part

        engine.splitByMaxBytesSorted(
                input,
                outputDir,
                maxBytes,
                new NewlineSeparator(io.copyBufferBytes()),
                io,
                StandardCharsets.UTF_8
        );

        // ---- Assertions ----

        // List generated files
        var parts = Files.list(outputDir)
                .filter(p -> p.getFileName().toString().startsWith("sortedSeq-"))
                .sorted()
                .toList();

        long inputSize = Files.size(input);

        long expectedParts = inputSize / maxBytes;
        if (inputSize % maxBytes != 0) {
            expectedParts++; // last partial part
        }

        assertEquals(expectedParts, parts.size(),
                "Number of generated parts does not match expected division");


        long totalSize = 0;

        for (int i = 0; i < parts.size(); i++) {
            long size = Files.size(parts.get(i));
            totalSize += size;

            if (i < parts.size() - 1) {
                // All parts except last should be approximately maxBytes
                assertTrue(
                        Math.abs(size - maxBytes) < 1024,
                        "Part " + (i + 1) + " size not close to expected maxBytes"
                );
            } else {
                // Last part should be <= maxBytes
                assertTrue(size <= maxBytes, "Last part should not exceed maxBytes");
            }
        }

        // Ensure total size matches original file
        assertEquals(inputSize, totalSize, "Sum of parts must equal original file size");


    }
}