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

        String filePrefix = "partBig-";
        IOConfig io = new IOConfig(
                8 * 1024 * 1024,   // copy buffer
                4,                 // parallelism
                false,             // preferSequential
                filePrefix,
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
                .filter(p -> p.getFileName().toString().startsWith(filePrefix))
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

        String filePrefix = "sortedMaxBytesParallel-";
        IOConfig io = new IOConfig(
                8 * 1024 * 1024,   // copy buffer
                2,                 // parallelism
                false,             // preferSequential
                filePrefix,
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
                .filter(p -> p.getFileName().toString().startsWith(filePrefix))
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

        String filePrefix = "sortedMaxBytesSeq-";
        IOConfig io = new IOConfig(
                8 * 1024 * 1024,   // copy buffer
                2,                 // parallelism
                true,             // preferSequential
                filePrefix,
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
                .filter(p -> p.getFileName().toString().startsWith(filePrefix))
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
    void shouldSplitAndSortParallelFileByParts() throws Exception {

        Path input = Path.of("src/test/resources/unsorted.txt");
        Path outputDir = tempDir;
        //Path outputDir = Path.of("src/test/resources/output");;

        Segment segment = new RangeSegment(0,10);
        KeySpec keySpec = new KeySpec(List.of(segment)); // sort by first 10 chars of each line

        SortedSplitEngine engine = new SortedSplitEngine(new SplitEngine(), keySpec, keySpec.comparator());

        String filePrefix = "sortedPartsParallel-";
        IOConfig io = new IOConfig(
                8 * 1024 * 1024,   // copy buffer
                2,                 // parallelism
                false,             // preferSequential
                filePrefix,
                ".txt"
        );

        int numberOfParts = 2;
        engine.splitByParts(
                input,
                outputDir,
                numberOfParts,
                new NewlineSeparator(io.copyBufferBytes()),
                io
        );

        // ---- Assertions ----

        // List generated files
        var parts = Files.list(outputDir)
                .filter(p -> p.getFileName().toString().startsWith(filePrefix))
                .sorted()
                .toList();

        long inputSize = Files.size(input);

        assertEquals(numberOfParts, parts.size(),
                "Number of generated parts does not match expected division");

        long totalSize = 0;

        for (int i = 0; i < parts.size(); i++) {
            long size = Files.size(parts.get(i));
            totalSize += size;

                // All parts except  should be approximately 60 % maxBytes
                assertTrue(
                        Math.abs(size) <= (inputSize*0.6),
                        "Part " + (i + 1) + " size not close to expected maxBytes"
                );
        }

        // Ensure total size matches original file
        // plus 1 byte because of the last line without newline char, which is still a line and should be included in the total size this is beacuse we split in two parts
        assertEquals(inputSize, totalSize+1, "Sum of parts must equal original file size");
    }

    @Test
    void shouldSplitAndSortSequentialFileByParts() throws Exception {

        Path input = Path.of("src/test/resources/unsorted.txt");
        Path outputDir = tempDir;
        //Path outputDir = Path.of("src/test/resources/output");;

        Segment segment = new RangeSegment(0,10);
        KeySpec keySpec = new KeySpec(List.of(segment)); // sort by first 10 chars of each line

        SortedSplitEngine engine = new SortedSplitEngine(new SplitEngine(), keySpec, keySpec.comparator());

        String filePrefix = "sortedPartsSeq-";
        IOConfig io = new IOConfig(
                8 * 1024 * 1024,   // copy buffer
                0,                 // parallelism
                true,             // preferSequential
                filePrefix,
                ".txt"
        );

        int numberOfParts = 2;
        engine.splitByParts(
                input,
                outputDir,
                numberOfParts,
                new NewlineSeparator(io.copyBufferBytes()),
                io
        );

        // ---- Assertions ----

        // List generated files
        var parts = Files.list(outputDir)
                .filter(p -> p.getFileName().toString().startsWith(filePrefix))
                .sorted()
                .toList();

        long inputSize = Files.size(input);

        assertEquals(numberOfParts, parts.size(),
                "Number of generated parts does not match expected division");

        long totalSize = 0;

        for (int i = 0; i < parts.size(); i++) {
            long size = Files.size(parts.get(i));
            totalSize += size;

            // All parts except  should be approximately 60 % maxBytes
            assertTrue(
                    Math.abs(size) <= (inputSize*0.6),
                    "Part " + (i + 1) + " size not close to expected maxBytes"
            );
        }

        // Ensure total size matches original file
        // plus 1 byte because of the last line without newline char, which is still a line and should be included in the total size this is beacuse we split in two parts
        assertEquals(inputSize, totalSize+1, "Sum of parts must equal original file size");
    }
}