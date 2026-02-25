package org.github.faberna.file.merge;

import org.github.faberna.file.split.model.Separator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Reads records from a file using a custom byte-sequence separator.
 * Returns record bytes without the separator.
 */
 final class ChunkRecordReader implements AutoCloseable {
    private final java.io.BufferedInputStream in;
    private final Separator sep;
    private final byte[] sepBytes;
    private final int sepLen;
    private final byte[] buf = new byte[256 * 1024];
    private int pos = 0;
    private int limit = 0;
    private boolean eof = false;

    private ChunkRecordReader(java.io.BufferedInputStream in, Separator separator) throws IOException {
        this.in = in;
        this.sep = separator;
        this.sepBytes = separator.bytes();
        this.sepLen =sepBytes.length;
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

            if (b == sepBytes[match]) {
                match++;
                if (match == sepLen) {
                    return out.toByteArray();
                }
                continue;
            }

            if (match > 0) {
                out.write(sepBytes, 0, match);
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