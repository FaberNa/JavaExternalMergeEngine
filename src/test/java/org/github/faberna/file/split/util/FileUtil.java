package org.github.faberna.file.split.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class FileUtil {
    private static final int DEFAULT_BUFFER_SIZE = 256 * 1024; // 256 KB

    public static String recognizeNewLineSeparatorForFile(Path file) {
        Objects.requireNonNull(file, "file");

        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            long fileSize = ch.size();
            if (fileSize <= 0) {
                return System.lineSeparator(); // fallback
            }

            ByteBuffer buf = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);
            long pos = 0;
            boolean pendingCR = false;

            while (pos < fileSize) {
                buf.clear();
                int read = ch.read(buf, pos);
                if (read <= 0) break;
                buf.flip();

                for (int i = 0; i < read; i++) {
                    byte b = buf.get(i);

                    if (pendingCR) {
                        if (b == (byte) '\n') {
                            return "\r\n";
                        }
                        return "\r";
                    }

                    if (b == (byte) '\n') {
                        return "\n";
                    }

                    if (b == (byte) '\r') {
                        if (i + 1 < read) {
                            byte next = buf.get(i + 1);
                            if (next == (byte) '\n') {
                                return "\r\n";
                            }
                            return "\r";
                        } else {
                            pendingCR = true;
                        }
                    }
                }

                pos += read;
            }

            if (pendingCR) {
                return "\r";
            }

            // No newline found, fallback to system default
            return System.lineSeparator();

        } catch (IOException e) {
            throw new RuntimeException("Failed to detect newline separator for file: " + file, e);
        }
    }
}
