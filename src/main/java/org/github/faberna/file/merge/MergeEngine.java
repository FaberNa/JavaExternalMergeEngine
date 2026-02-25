package org.github.faberna.file.merge;

import org.github.faberna.file.segment.model.KeySpec;
import org.github.faberna.file.split.model.Separator;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public final class MergeEngine {


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
        if (keySpec == null) {
            throw new IllegalArgumentException("keySpec is required");
        }
        kWayMerge(sortedChunks, outputFile, keySpec.comparator(), charset, recordSeparator);
    }

    public static void kWayMerge(
            List<Path> sortedChunks,
            Path outputFile,
            Comparator<String> lineComparator,
            Charset charset,
            Separator recordSeparator
    ) throws IOException {
        if (lineComparator == null) {
            throw new IllegalArgumentException("lineComparator is required");
        }
        if (charset == null) {
            throw new IllegalArgumentException("charset is required");
        }
        if (recordSeparator == null || recordSeparator.length() == 0) {
            throw new IllegalArgumentException("recordSeparatorBytes is required");
        }

        List<ChunkRecordReader> readers = new ArrayList<>(sortedChunks.size());
        try {
            for (Path p : sortedChunks) {
                readers.add(ChunkRecordReader.open(p, recordSeparator));
            }

            PriorityQueue<HeapItem> pq = new PriorityQueue<>((a, b) -> {
                int c = lineComparator.compare(a.line, b.line);
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

            try (var out = Files.newOutputStream(outputFile)) {
                while (!pq.isEmpty()) {
                    HeapItem smallest = pq.poll();

                    out.write(smallest.recordBytes);
                    out.write(recordSeparator.bytes());

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
                } catch (IOException ignored) {
                }
            }
        }
    }

    /** Common default separators. */
    public static final class DefaultSeparators {
        private DefaultSeparators() {}
        public static final byte[] LF = new byte[]{'\n'};
        public static final byte[] CRLF = new byte[]{'\r', '\n'};
        public static final byte[] CR = new byte[]{'\r'};
    }

    /**
     * Reads records from a file using a custom byte-sequence separator.
     * Returns record bytes without the separator.
     */
    static final class ChunkRecordReader implements AutoCloseable {
        private final java.io.BufferedInputStream in;
        private final Separator sep;
        private final byte[] buf = new byte[64 * 1024];
        private int pos = 0;
        private int limit = 0;
        private boolean eof = false;

        private ChunkRecordReader(java.io.BufferedInputStream in, Separator separator) throws IOException {
            this.in = in;
            this.sep = separator;
        }

        static ChunkRecordReader open(Path p, Separator sep) throws IOException {
            return new ChunkRecordReader(new java.io.BufferedInputStream(Files.newInputStream(p)), sep);
        }

        byte[] nextRecord() throws IOException {
            if (eof && pos >= limit) return null;

            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(256);
            int match = 0;

            while (true) {
                if (pos >= limit) {
                    if (eof) {
                        return out.size() == 0 ? null : out.toByteArray();
                    }
                    limit = in.read(buf);
                    pos = 0;
                    if (limit == -1) {
                        eof = true;
                        limit = 0;
                        continue;
                    }
                }

                byte b = buf[pos++];

                if (b == sep.bytes()[match]) {
                    match++;
                    if (match == sep.length()) {
                        return out.toByteArray();
                    }
                    continue;
                }

                if (match > 0) {
                    out.write(sep.bytes(), 0, match);
                    match = 0;
                }
                out.write(b);
            }
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    /**
     * Heap element: decoded line for comparison + raw record bytes for output.
     */
    static final class HeapItem {
        final String line;
        final byte[] recordBytes;
        final int chunkIndex;
        final long seq;

        HeapItem(String line, byte[] recordBytes, int chunkIndex, long seq) {
            this.line = line;
            this.recordBytes = recordBytes;
            this.chunkIndex = chunkIndex;
            this.seq = seq;
        }
    }
}
