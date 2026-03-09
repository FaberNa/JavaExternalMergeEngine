package org.github.faberna.file.split.splitter;

import org.github.faberna.file.split.SplitEngine;
import org.github.faberna.file.split.config.IOConfig;
import org.github.faberna.file.split.model.NewlineSeparator;
import org.github.faberna.file.split.model.Separator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class ParallelSplitterTest {


    @TempDir
    Path tempDir;

    @Test
    void shouldSplitParallelFileByMaxBytes() throws IOException {
        SplitEngine engine = new SplitEngine();

        Path input = Path.of("src/test/resources/unsorted.txt");

        Path outDir = tempDir;
        Separator sep = new NewlineSeparator(1, null);
        String filePrefix = "part-";
        int maxBytesPerPart = 256 * 1024;
        IOConfig io = new IOConfig(8*1024*1024, 4, false, filePrefix, ".txt");

         engine.splitByMaxBytes(input, outDir, maxBytesPerPart, sep, io);
        // List generated files
        var parts = Files.list(outDir)
                .filter(p -> p.getFileName().toString().startsWith(filePrefix))
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
}
