package org.github.faberna.file.split;

import org.github.faberna.file.split.config.IOConfig;
import org.github.faberna.file.split.plan.SplitPlan;
import org.github.faberna.file.split.model.Separator;
import org.github.faberna.file.split.plan.SplitPlanner;
import org.github.faberna.file.split.sorter.InMemorySortingPartWriter;
import org.github.faberna.file.split.sorter.PartWriter;
import org.github.faberna.file.segment.model.KeySpec;
import org.github.faberna.file.split.sorter.PartWriterFactory;
import org.github.faberna.file.split.splitter.ParallelRangeSplitter;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;

/**
 * Split + Sort (per-part) engine.
 *
 * Guarantees:
 * - Splits the input into part files using SplitEngine.
 * - Each part is sorted IN-MEMORY before being written (contextual split+sort), using the provided KeySpec.
 * - Preserves original line endings (LF/CRLF/CR) because the PartWriter receives the detected ending per line.
 *
 * Notes:
 * - This produces "sorted runs" (each part internally sorted). Global ordering requires a merge step.
 * - For now, sorting is supported only in sequential mode (IOConfig.preferSequential() = true).
 */
public final class SortedSplitEngine {


    private final SplitEngine splitEngine;
    private final KeySpec keySpec;
    private final Comparator<String> keyComparator;
    private final SplitPlanner planner = new SplitPlanner();
    private final ParallelRangeSplitter parallel = new ParallelRangeSplitter();

    public SortedSplitEngine(SplitEngine splitEngine, KeySpec keySpec, Comparator<String> keyComparator) {
        this.splitEngine = Objects.requireNonNull(splitEngine, "splitEngine is required");
        this.keySpec = Objects.requireNonNull(keySpec, "keySpec is required");
        this.keyComparator = Objects.requireNonNull(keyComparator, "keyComparator is required");
    }

    /**
     * Split the input file by max bytes (record-safe), then sort each produced part in-memory.
     */
    public void splitByMaxBytes(
            Path input,
            Path outputDir,
            long maxBytesPerPart,
            Separator separator,
            IOConfig io
    ) throws IOException {
        splitByMaxBytesSorted(input, outputDir, maxBytesPerPart, separator, io, StandardCharsets.UTF_8);
    }

    /**
     * Split the input file by max bytes (record-safe), then sort each produced part in-memory.
     */
     void splitByMaxBytesSorted(
            Path input,
            Path outputDir,
            long maxBytesPerPart,
            Separator separator,
            IOConfig io,
            Charset charset
    ) throws IOException {

        requireInputs(input, outputDir, separator, io, charset);
        if (maxBytesPerPart <= 0){
            throw new IllegalArgumentException("maxBytesPerPart must be > 0");
        }

         PartWriterFactory factory = () -> new InMemorySortingPartWriter(keySpec, keyComparator, charset);;

         if (io.preferSequential()) {
             splitEngine.splitByMaxBytes(input, outputDir, maxBytesPerPart, separator, io, factory.create());
             return;
         }

         SplitPlan plan = planner.planByMaxBytes(
                 input,
                 outputDir,
                 maxBytesPerPart,
                 separator
         );

         if (io.parallelism() > 1) {
             parallel.execute(plan, io,factory);
         } else {
             // Reuse the parallel executor with a single thread (no separate SequentialRangeSplitter needed)
             parallel.execute(plan, new IOConfig(
                     io.copyBufferBytes(),
                     1,
                     false,
                     io.filePrefix(),
                     io.fileExtension()
             ),factory);
         }
    }

    /**
     * Split the input file into the requested number of parts (record-safe), then sort each produced part in-memory.
     */
    public void splitByParts(
            Path input,
            Path outputDir,
            int parts,
            Separator separator,
            IOConfig io
    ) throws IOException {
        splitByPartsSorted(input, outputDir, parts, separator, io, StandardCharsets.UTF_8);
    }

    /**
     * Split the input file into the requested number of parts (record-safe), then sort each produced part in-memory.
     */
    void splitByPartsSorted(
            Path input,
            Path outputDir,
            int parts,
            Separator separator,
            IOConfig io,
            Charset charset
    ) throws IOException {

        requireInputs(input, outputDir, separator, io, charset);
        if (parts <= 0) throw new IllegalArgumentException("parts must be > 0");

        if (io.preferSequential()) {
            // 2) Sort each part file using KeySpec
            // If you want zero-allocation comparisons, use keySpec.comparator().
            // If you want to allow a custom keyComparator on the materialized key, use keySpec.comparator(keyComparator).
            PartWriter writer = new InMemorySortingPartWriter(keySpec, keyComparator, charset);
            splitEngine.splitByParts(input, outputDir, parts, separator, io, writer);
        }else {

        SplitPlan plan = planner.planByParts(
                input,
                outputDir,
                parts,
                separator
        );

        PartWriterFactory factory =
                () -> new InMemorySortingPartWriter(keySpec, keyComparator, charset);
        if (io.parallelism() > 1) {
            parallel.execute(plan, io,factory);
        } else {
            // Reuse the parallel executor with a single thread (no separate SequentialRangeSplitter needed)
            parallel.execute(plan, new IOConfig(
                    io.copyBufferBytes(),
                    1,
                    false,
                    io.filePrefix(),
                    io.fileExtension()
            ),factory);
        }
        }



    }

    // ------------------------- helpers -------------------------

    private static void requireInputs(Path input, Path outputDir, Separator separator, IOConfig io, Charset charset) {
        Objects.requireNonNull(input, "input is required");
        Objects.requireNonNull(outputDir, "outputDir is required");
        Objects.requireNonNull(separator, "separator is required");
        Objects.requireNonNull(io, "io is required");
        Objects.requireNonNull(charset, "charset is required");
    }


}