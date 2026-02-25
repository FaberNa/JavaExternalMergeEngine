package org.github.faberna.file;

import org.github.faberna.file.merge.MergeEngine;
import org.github.faberna.file.segment.model.KeySpec;
import org.github.faberna.file.segment.model.RangeSegment;
import org.github.faberna.file.segment.model.Segment;
import org.github.faberna.file.split.SortedSplitEngine;
import org.github.faberna.file.split.SplitEngine;
import org.github.faberna.file.split.config.IOConfig;
import org.github.faberna.file.split.model.NewlineSeparator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SortAndMergeFileTest {

    @TempDir
    Path tempDir;

   //@AfterEach
   void cleanUp() throws IOException {
       Path outputDir = Path.of("src/test/resources/output");;
       // Clean up tempDir after each test
       Files.walk(outputDir)
               .filter(Files::isRegularFile)
               .forEach(p -> {
                   try {
                       Files.delete(p);
                   } catch (IOException e) {
                       throw new UncheckedIOException(e);
                   }
               });
   }

    @Test
    @EnabledIfSystemProperty(named = "run.large.tests", matches = "true")
    void kWayMerge_shouldMergeAndSplitParallelRealFileUsingKeySpecAndNewLineSeparator() throws IOException {
        Path input = Path.of("src/test/resources/test_1gb.txt");

        Path outputDir = Path.of("src/test/resources/output");;
        //Path outputDir = tempDir;

        Segment segment = new RangeSegment(0,10);
        KeySpec keySpec = new KeySpec(List.of(segment)); // sort by first 10 chars of each line

        SortedSplitEngine engine = new SortedSplitEngine(new SplitEngine(), keySpec, keySpec.comparator());

        String filePrefix = "sortedMaxBytesParallel-";
        IOConfig io = new IOConfig(
                8 * 1024 * 1024,   // copy buffer
                4,                 // parallelism
                false,             // preferSequential
                filePrefix,
                ".txt"
        );

        long maxBytes = 100L * 1024 * 1024; // 100MB per part

        NewlineSeparator separator = new NewlineSeparator(io.copyBufferBytes());
        engine.splitByMaxBytes(
                input,
                outputDir,
                maxBytes,
                separator,
                io
        );

        var parts = Files.list(outputDir)
                .filter(p -> p.getFileName().toString().startsWith(filePrefix))
                .sorted()
                .toList();


        Path out = outputDir.resolve("out-1gb-parallel.txt");

        // When
        MergeEngine.kWayMerge(parts, out, keySpec, StandardCharsets.UTF_8, separator);

        // Then: output is globally sorted and separator preserved
        byte[] outBytes = Files.readAllBytes(out);

        // must end with separator
        assertTrue(outBytes.length > 0, "output should not be empty");
        assertEquals((byte) '\n', outBytes[outBytes.length - 1], "output should end with record separator");

        String outText = new String(outBytes, StandardCharsets.UTF_8);

        // because string ends with '|', split() returns last token empty -> ignore it
        long totalBytes = parts.stream()
                .mapToLong(p -> {
                    try {
                        return Files.size(p);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .sum();
        assertThat(totalBytes).isLessThanOrEqualTo(Files.size(out));
    }

    @Test
    @EnabledIfSystemProperty(named = "run.large.tests", matches = "true")
    void kWayMerge_shouldMergeAndSplitSequentiallyRealFileUsingKeySpecAndNewLineSeparator() throws IOException {
        Path input = Path.of("src/test/resources/test_1gb.txt");

        Path outputDir = Path.of("src/test/resources/output");;
        //Path outputDir = tempDir;

        Segment segment = new RangeSegment(0,10);
        KeySpec keySpec = new KeySpec(List.of(segment)); // sort by first 10 chars of each line

        SortedSplitEngine engine = new SortedSplitEngine(new SplitEngine(), keySpec, keySpec.comparator());

        String filePrefix = "sortedMaxBytesSequentially-";
        IOConfig io = new IOConfig(
                8 * 1024 * 1024,   // copy buffer
                0,                 // parallelism
                true,             // preferSequential
                filePrefix,
                ".txt"
        );

        long maxBytes = 100L * 1024 * 1024; // 100MB per part

        NewlineSeparator separator = new NewlineSeparator(io.copyBufferBytes());
        engine.splitByMaxBytes(
                input,
                outputDir,
                maxBytes,
                separator,
                io
        );

        var parts = Files.list(outputDir)
                .filter(p -> p.getFileName().toString().startsWith(filePrefix))
                .sorted()
                .toList();


        Path out = outputDir.resolve("out-1gb-seq.txt");

        // When
        MergeEngine.kWayMerge(parts, out, keySpec, StandardCharsets.UTF_8, separator);

        // Then: output is globally sorted and separator preserved
        byte[] outBytes = Files.readAllBytes(out);

        // must end with separator
        assertTrue(outBytes.length > 0, "output should not be empty");
        assertEquals((byte) '\n', outBytes[outBytes.length - 1], "output should end with record separator");

        String outText = new String(outBytes, StandardCharsets.UTF_8);

        // because string ends with '|', split() returns last token empty -> ignore it
        long totalBytes = parts.stream()
                .mapToLong(p -> {
                    try {
                        return Files.size(p);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .sum();
        assertThat(totalBytes).isLessThanOrEqualTo(Files.size(out));
    }



    @Test
    void kWayMerge_shouldMergeRealLittleFileUsingKeySpecAndNewLineSeparator() throws IOException {
        //Path input = Path.of("src/test/resources/oneline.txt");
        Path input = Path.of("src/test/resources/unsorted.txt");

        //Path outputDir = Path.of("src/test/resources/output");;
        Path outputDir = tempDir;

        Segment segment = new RangeSegment(0,10);
        KeySpec keySpec = new KeySpec(List.of(segment)); // sort by first 10 chars of each line

        SortedSplitEngine engine = new SortedSplitEngine(new SplitEngine(), keySpec, keySpec.comparator());

        String filePrefix = "sortedAndMergeByPartParallel-";
        IOConfig io = new IOConfig(
                8 * 1024 * 1024,   // copy buffer
                4,                 // parallelism
                false,             // preferSequential
                filePrefix,
                ".txt"
        );

        int numParts = 2; // split into 2 parts
        NewlineSeparator separator = new NewlineSeparator(io.copyBufferBytes());
        engine.splitByParts(
                input,
                outputDir,
                numParts,
                separator,
                io
        );

        var parts = Files.list(outputDir)
                .filter(p -> p.getFileName().toString().startsWith(filePrefix))
                .sorted()
                .toList();


        Path out = outputDir.resolve("out-little.txt");

        // When
        MergeEngine.kWayMerge(parts, out, keySpec, StandardCharsets.UTF_8, separator);

        // Then: output is globally sorted and separator preserved
        byte[] outBytes = Files.readAllBytes(out);

        // must end with separator
        assertTrue(outBytes.length > 0, "output should not be empty");
        assertEquals((byte) '\n', outBytes[outBytes.length - 1], "output should end with record separator");

        String outText = new String(outBytes, StandardCharsets.UTF_8);

        // split by separator and drop trailing empty token caused by last separator
        String[] tokens = outText.split("\\\n");
        // because string ends with '|', split() returns last token empty -> ignore it
        long totalBytes = parts.stream()
                .mapToLong(p -> {
                    try {
                        return Files.size(p);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .sum();
        assertThat(totalBytes).isLessThanOrEqualTo(Files.size(out));
        assertThat(parts.size()).isEqualTo(numParts);
    }
}
