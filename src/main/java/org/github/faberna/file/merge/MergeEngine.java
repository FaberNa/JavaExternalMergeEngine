package org.github.faberna.file.merge;

import org.github.faberna.file.merge.model.HeapItem;
import org.github.faberna.file.segment.model.KeySpec;
import org.github.faberna.file.split.model.Separator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public final class MergeEngine {


    private static final Logger log = LoggerFactory.getLogger(MergeEngine.class);

    private MergeEngine() {
        /* This utility class should not be instantiated */
    }



    /**
     * K-way merge using KeySpec ordering and the exact record separator used in the chunk files.
     */
    public static void kWayMerge(
            List<Path> sortedChunks,
            Path outputFile,
            KeySpec keySpec,
            Charset charset,
            Separator recordSeparator
    ) throws IOException {
        log.info("kWayMerge start");
        if (keySpec == null) {
            throw new IllegalArgumentException("keySpec is required");
        }
        kWayMerge(sortedChunks, outputFile, keySpec.comparator(), charset, recordSeparator);
        log.info("kWayMerge end");
    }

    public static void kWayMerge(
            List<Path> sortedChunks,
            Path outputFile,
            Comparator<String> keySpecComparator,
            Charset charset,
            Separator recordSeparator
    ) throws IOException {
        checkParameters(keySpecComparator, charset, recordSeparator);
        final byte[] bytes = recordSeparator.bytes();

        List<ChunkRecordReader> readers = new ArrayList<>(sortedChunks.size());
        try {
            for (Path p : sortedChunks) {
                readers.add(ChunkRecordReader.open(p, recordSeparator));
            }

            PriorityQueue<HeapItem> pq = new PriorityQueue<>((a, b) -> {
                int c = keySpecComparator.compare(a.line, b.line);
                if (c != 0) return c;
                return Long.compare(a.seq, b.seq);
            });

            long seq = 0;
            for (int i = 0; i < readers.size(); i++) {
                byte[] rec = readers.get(i).nextRecord();
                if (rec != null) {
                    pq.add(new HeapItem(new String(rec, charset), rec, i, seq++));
                }
            }
            try (var out = new java.io.BufferedOutputStream(Files.newOutputStream(outputFile), 1 << 20)) {
                while (!pq.isEmpty()) {
                    HeapItem smallest = pq.poll();

                    out.write(smallest.recordBytes);

                    out.write(bytes);

                    byte[] next = readers.get(smallest.chunkIndex).nextRecord();
                    if (next != null) {
                        pq.add(new HeapItem(new String(next, charset), next, smallest.chunkIndex, seq++));
                    }
                }
            }

        } finally {
            for (ChunkRecordReader r : readers) {
                try {
                    if (r != null) r.close();
                } catch (IOException _) {
                   // ignore close exceptions
                }
            }
        }
    }

    private static void checkParameters(Comparator<String> keySpecComparator, Charset charset, Separator recordSeparator) {
        if (keySpecComparator == null) {
            throw new IllegalArgumentException("keySpecComparator is required");
        }
        if (charset == null) {
            throw new IllegalArgumentException("charset is required");
        }
        if (recordSeparator == null || recordSeparator.length() == 0) {
            throw new IllegalArgumentException("record Separator is required");
        }
    }

}
