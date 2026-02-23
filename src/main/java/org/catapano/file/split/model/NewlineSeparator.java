package org.catapano.file.split.model;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * Supports LF (\n), CRLF (\r\n), and CR (\r).
 * Returns boundary offset immediately AFTER the newline bytes.
 */
public final class NewlineSeparator implements Separator {

    private final int bufferSize;

    public NewlineSeparator(int bufferSize) {
        if (bufferSize <= 0) throw new IllegalArgumentException("bufferSize must be > 0");
        this.bufferSize = bufferSize;
    }

    @Override
    public long findNextSeparatorEnd(FileChannel ch, long from, long fileSize) throws IOException {
        Objects.requireNonNull(ch, "ch");
        if (from < 0) from = 0;
        if (from >= fileSize) return -1;

        // Direct buffer usually gives better IO performance
        ByteBuffer buf = ByteBuffer.allocateDirect(bufferSize);

        long pos = from;

        // If the previous buffer ended with '\r', we need to decide whether it was CRLF.
        boolean pendingCR = false;

        while (pos < fileSize) {
            buf.clear();
            int read = ch.read(buf, pos);
            if (read <= 0) return -1;

            buf.flip();

            for (int i = 0; i < read; i++) {
                byte b = buf.get(i);

                if (pendingCR) {
                    // Previous buffer ended with '\r'
                    // If current first byte is '\n', then separator is CRLF across buffers.
                    // Otherwise it was a lone CR.
                    if (b == (byte) '\n') {
                        return pos + i + 1; // after '\n'
                    } else {
                        return pos; // CR ended exactly at pos (start of this buffer)
                    }
                }

                if (b == (byte) '\n') {
                    return pos + i + 1; // after LF
                }

                if (b == (byte) '\r') {
                    // Could be CRLF or CR
                    if (i + 1 < read) {
                        byte next = buf.get(i + 1);
                        if (next == (byte) '\n') {
                            return pos + i + 2; // after CRLF
                        }
                        return pos + i + 1; // after CR
                    } else {
                        // '\r' is last byte of this buffer => decide in next loop
                        pendingCR = true;
                    }
                }
            }

            // If we end the buffer with pendingCR=true, next loop will resolve it.
            pos += read;
        }

        // EOF reached: if file ends with a trailing CR, consider it a separator ending at EOF.
        if (pendingCR) return fileSize;

        return -1;
    }
}