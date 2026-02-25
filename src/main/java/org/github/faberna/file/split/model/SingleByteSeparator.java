package org.github.faberna.file.split.model;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * Separator is a single byte (e.g. '$' = 0x24, '%' = 0x25, or 0x1E record separator).
 * Returns boundary offset immediately AFTER the separator byte.
 */
public final class SingleByteSeparator implements Separator {

    private final byte sep;

    @Override
    public byte[] bytes() {
        return new byte[]{this.sep}; // usa il nome reale del campo
    }

    private final int bufferSize;
    public SingleByteSeparator(byte sep, int bufferSize) {
        if (bufferSize <= 0) throw new IllegalArgumentException("bufferSize must be > 0");
        this.sep = sep;
        this.bufferSize = bufferSize;
    }

    public byte getSep() {
        return sep;
    }

    @Override
    public long findNextSeparatorEnd(FileChannel ch, long from, long fileSize) throws IOException {
        Objects.requireNonNull(ch, "ch");
        if (from < 0) from = 0;
        if (from >= fileSize) return -1;

        ByteBuffer buf = ByteBuffer.allocateDirect(bufferSize);
        long pos = from;

        while (pos < fileSize) {
            buf.clear();
            int read = ch.read(buf, pos);
            if (read <= 0) return -1;

            buf.flip();

            for (int i = 0; i < read; i++) {
                if (buf.get(i) == sep) {
                    return pos + i + 1;
                }
            }

            pos += read;
        }

        return -1;
    }
}