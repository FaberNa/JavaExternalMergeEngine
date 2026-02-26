package org.github.faberna.file.split;

import org.github.faberna.file.split.config.IOConfig;
import org.github.faberna.file.split.plan.SplitPlan;
import org.github.faberna.file.split.model.Separator;
import org.github.faberna.file.split.plan.SplitPlanner;
import org.github.faberna.file.split.sorter.PartWriter;
import org.github.faberna.file.split.splitter.ParallelRangeSplitter;
import org.github.faberna.file.split.splitter.SequentialStreamingSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

 public final class SplitEngine {

     private static final Logger log = LoggerFactory.getLogger(SplitEngine.class);

     private final SplitPlanner planner = new SplitPlanner();
    private final ParallelRangeSplitter parallel = new ParallelRangeSplitter();
    private final SequentialStreamingSplitter streaming = new SequentialStreamingSplitter();

    public void splitByMaxBytes(
            Path input,
            Path outputDir,
            long maxBytes,
            Separator sep,
            IOConfig io,
            PartWriter partWriter
    ) throws IOException {
        log.info("splitByMaxBytes sequential start");
        if (io == null) io = IOConfig.defaults();
        if (partWriter == null) throw new IllegalArgumentException("partWriter is required");
        if (io.preferSequential()) {
            streaming.splitByMaxBytes(input, outputDir, maxBytes, sep, io, partWriter);
            log.info("splitByMaxBytes sequential end");
            return;
        }


        // For now, PartWriter mode is supported only in sequential streaming.
        throw new UnsupportedOperationException("PartWriter mode currently supported only with IOConfig.preferSequential()=true");
    }

     /**
      * Split the input file by max bytes (record-safe).
      * @param input
      * @param outputDir
      * @param maxBytes
      * @param sep
      * @param io
      * @throws IOException
      */
    public void splitByMaxBytes(
            Path input,
            Path outputDir,
            long maxBytes,
            Separator sep,
            IOConfig io
    ) throws IOException {

        if (io.preferSequential()) {
            streaming.splitByMaxBytes(input, outputDir, maxBytes, sep, io);
            return;
        }

        SplitPlan plan = planner.planByMaxBytes(
                input,
                outputDir,
                maxBytes,
                sep
        );

        if (io.parallelism() > 1) {
            parallel.execute(plan, io);
        } else {
            // Reuse the parallel executor with a single thread (no separate SequentialRangeSplitter needed)
            parallel.execute(plan, new IOConfig(
                    io.copyBufferBytes(),
                    1,
                    false,
                    io.filePrefix(),
                    io.fileExtension()
            ));
        }
    }

    /**
     * Split the input file into a given number of parts (record-safe).
     * @param input
     * @param outputDir
     * @param parts
     * @param sep
     * @param io
     * @throws IOException
     */
    public void splitByParts(
            Path input,
            Path outputDir,
            int parts,
            Separator sep,
            IOConfig io,
            PartWriter partWriter
    ) throws IOException {

        if (io == null) io = IOConfig.defaults();
        if (partWriter == null) throw new IllegalArgumentException("partWriter is required");
        if (io.preferSequential()) {
            streaming.splitByParts(input, outputDir, parts, sep, io, partWriter);
            return;
        }

        // For now, PartWriter mode is supported only in sequential streaming.
        throw new UnsupportedOperationException("PartWriter mode currently supported only with IOConfig.preferSequential()=true");
    }

    /**
     * Split the input file into a given number of parts (record-safe).
     * @param input
     * @param outputDir
     * @param parts
     * @param sep
     * @param io
     * @throws IOException
     */
    public void splitByParts(
            Path input,
            Path outputDir,
            int parts,
            Separator sep,
            IOConfig io
    ) throws IOException {

        if (io == null) io = IOConfig.defaults();

        if (io.preferSequential()) {
            streaming.splitByParts(input, outputDir, parts, sep, io);
            return;
        }

        SplitPlan plan = planner.planByParts(
                input,
                outputDir,
                parts,
                sep
        );

        if (io.parallelism() > 1) {
            parallel.execute(plan, io);
        } else {
            parallel.execute(plan, new IOConfig(
                    io.copyBufferBytes(),
                    1,
                    false,
                    io.filePrefix(),
                    io.fileExtension()
            ));
        }
    }

}