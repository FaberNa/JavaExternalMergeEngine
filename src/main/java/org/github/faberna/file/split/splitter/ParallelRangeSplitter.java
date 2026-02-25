package org.github.faberna.file.split.splitter;

import org.github.faberna.file.split.SplitEngine;
import org.github.faberna.file.split.config.IOConfig;
import org.github.faberna.file.split.plan.SplitPlan;
import org.github.faberna.file.split.model.LineEnding;
import org.github.faberna.file.split.model.Range;
import org.github.faberna.file.split.sorter.InMemorySortingPartWriter;
import org.github.faberna.file.split.sorter.PartWriter;
import org.github.faberna.file.split.sorter.PartWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.*;


import static java.nio.file.StandardOpenOption.*;
import static org.github.faberna.file.split.SplitUtil.emitLine;

/**
 * A simple splitter that uses FileChannel.transferTo to copy byte ranges in parallel.
 * <p>
 * This is efficient for local files on spinning disks or SSDs, but may not perform well on network filesystems.
 * For NAS, consider using a single-threaded streaming copy instead.
 */
public final class ParallelRangeSplitter {
    private static final Logger log = LoggerFactory.getLogger(ParallelRangeSplitter.class);

    public void execute(SplitPlan plan, IOConfig io) throws IOException {
        log.info("Starting parallel splitter");
        if (plan == null) throw new IllegalArgumentException("plan is required");
        if (io == null) io = IOConfig.defaults();

        Files.createDirectories(plan.outputDir());

        List<Range> parts = plan.parts();
        if (parts == null || parts.isEmpty()) return;

        try (FileChannel in = FileChannel.open(plan.input(), StandardOpenOption.READ)) {
            int threads = getThreads(io, parts);

            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>(parts.size());

            for (int i = 0; i < parts.size(); i++) {
                int idx = i;
                Range r = parts.get(i);

                Path out = plan.outputDir().resolve(
                        String.format(Locale.ROOT, "%s%04d%s", io.filePrefix(), idx + 1, io.fileExtension())
                );
                // every thread copy and sort only a strictly range of bytes, so no need to synchronize access to the input channel
                futures.add(pool.submit(() -> {
                    try (FileChannel outCh = FileChannel.open(out, WRITE, CREATE, TRUNCATE_EXISTING)) {
                        copyRange(in, outCh, r.startInclusive(), r.endExclusive());
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                }));
            }
        doShutdown(pool, futures);

        }
        log.info("Finished parallel splitter");
    }



    public void execute(SplitPlan plan, IOConfig io, PartWriterFactory factory) throws IOException {
        log.info("Starting parallel splitter");
        if (plan == null) throw new IllegalArgumentException("plan is required");
        if (io == null) io = IOConfig.defaults();
        if (factory == null) throw new IllegalArgumentException("factory is required");
        final IOConfig ioFinal = io;
        final int copyBufferBytes = ioFinal.copyBufferBytes();

        Files.createDirectories(plan.outputDir());

        List<Range> parts = plan.parts();
        if (parts == null || parts.isEmpty()) return;

        try (FileChannel in = FileChannel.open(plan.input(), StandardOpenOption.READ)) {
            int threads = getThreads(io, parts);

            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>(parts.size());

            for (int i = 0; i < parts.size(); i++) {
                int idx = i;
                Range r = parts.get(i);

                Path out = plan.outputDir().resolve(
                        String.format(Locale.ROOT, "%s%04d%s", io.filePrefix(), idx + 1, io.fileExtension())
                );

                futures.add(pool.submit(() -> {
                    PartWriter writer = factory.create();
                    if (writer == null) throw new IllegalStateException("PartWriterFactory returned null writer");
                    InMemorySortingPartWriter inMemorySortingPartWriter = (InMemorySortingPartWriter) writer;
                    try {
                        processRangeAsLines(in, r.startInclusive(), r.endExclusive(), copyBufferBytes, inMemorySortingPartWriter.getCharset(), writer);
                        writer.endPart(out);
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                }));
            }
            doShutdown(pool, futures);
        }
        log.info("Finished parallel splitter");
    }

    /** Determine the number of threads to use based on IOConfig and number of parts.
     * If io.parallelism() is set to a positive value, use that.
     * Otherwise, use the minimum of available processors and number of parts to avoid oversubscription.
     * @param io
     * @param parts
     * @return
     **/
    private static int getThreads(IOConfig io, List<Range> parts) {
        return io.parallelism() > 0
                ? io.parallelism()
                : Math.min(Runtime.getRuntime().availableProcessors(), parts.size());
    }


    private void doShutdown(ExecutorService pool,List<Future<?>> futures ) throws IOException {
        pool.shutdown();

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while splitting", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof CompletionException ce && ce.getCause() != null) {
                    cause = ce.getCause();
                }
                if (cause instanceof IOException ioe) throw ioe;
                throw new IOException("Split failed", cause);
            }
        }
    }

    /**
     * Reads bytes in the range [start..end) and emits lines to the writer preserving LF/CRLF/CR.
     * Assumes the range boundaries are record-safe (planner should end ranges after a line separator).
     * This method extract lines from the byte range, detects their original line endings, and emits them to the PartWriter.
     */
    private static void processRangeAsLines(
            FileChannel in,
            long start,
            long end,
            int bufferSize,
            Charset charset,
            PartWriter writer
    ) throws IOException {
        if (start < 0 || end < start) throw new IllegalArgumentException("Invalid range: [" + start + "," + end + ")");
        if (bufferSize <= 0) throw new IllegalArgumentException("bufferSize must be > 0");

        ByteBuffer buf = ByteBuffer.allocateDirect(bufferSize);
        ByteArrayOutputStream lineBuf = new ByteArrayOutputStream(1024);

        boolean pendingCR = false;
        long pos = start;

        while (pos < end) {
            buf.clear();

            int toRead = (int) Math.min((long) bufferSize, end - pos);
            buf.limit(toRead);

            int read = in.read(buf, pos);
            if (read <= 0) break;
            buf.flip();

            for (int i = 0; i < read; i++) {
                byte b = buf.get(i);

               // Resolve CR at end of previous buffer
                if (pendingCR) {
                    if (b == (byte) '\n') {
                        emitLine(writer, lineBuf, LineEnding.CRLF, charset);
                        pendingCR = false;
                        continue;
                    } else {
                        // Lone CR ended between buffers
                        emitLine(writer, lineBuf, LineEnding.CR, charset);
                        pendingCR = false;
                        i--; // re-process current byte as regular content
                        continue;
                    }
                }

                if (b == (byte) '\n') {
                    emitLine(writer, lineBuf, LineEnding.LF, charset);
                } else if (b == (byte) '\r') {
                    if (i + 1 < read) {
                        byte next = buf.get(i + 1);
                        if (next == (byte) '\n') {
                            emitLine(writer, lineBuf, LineEnding.CRLF, charset);
                            i++; // consume '\n'
                        } else {
                            emitLine(writer, lineBuf, LineEnding.CR, charset);
                        }
                    } else {
                        // CR at end of this buffer: decide in next buffer
                        pendingCR = true;
                    }
                // Resolve CR carried from previous buffer
                } else {
                    lineBuf.write(b);
                }
            }

            pos += read;
        }

        // If the range ended with a pending CR, treat it as a CR terminator.
        if (pendingCR) {
            emitLine(writer, lineBuf, LineEnding.CR, charset);
            pendingCR = false;
        }

        // If anything remains (range not perfectly record-aligned), emit as last line without terminator.
        if (lineBuf.size() > 0) {
            emitLine(writer, lineBuf, LineEnding.NONE, charset);
        }
    }


    /**
     * Copy the specified byte range from the input channel to the output channel using transferTo. an API NIO provides for efficient file copying ( zero-copy )
     * @param in
     * @param out
     * @param start
     * @param end
     * @throws IOException
     */
    private static void copyRange(FileChannel in, FileChannel out, long start, long end) throws IOException {
        if (start < 0 || end < start) throw new IllegalArgumentException("Invalid range: [" + start + "," + end + ")");
        long remaining = end - start;
        long pos = start;

        while (remaining > 0) {
            long transferred = in.transferTo(pos, remaining, out);
            if (transferred <= 0) {
                throw new IOException("transferTo made no progress at pos=" + pos + ", remaining=" + remaining);
            }
            pos += transferred;
            remaining -= transferred;
        }
    }
}