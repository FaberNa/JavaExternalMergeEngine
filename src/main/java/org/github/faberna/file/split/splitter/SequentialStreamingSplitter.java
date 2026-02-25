package org.github.faberna.file.split.splitter;

import org.github.faberna.file.split.config.IOConfig;
import org.github.faberna.file.split.model.LineEnding;
import org.github.faberna.file.split.model.NewlineSeparator;
import org.github.faberna.file.split.model.Separator;
import org.github.faberna.file.split.model.SingleByteSeparator;
import org.github.faberna.file.split.sorter.PartWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Locale;

import static java.nio.file.StandardOpenOption.*;
import static org.github.faberna.file.split.SplitUtil.emitLineBytes;

public final class SequentialStreamingSplitter {

    /** 1-pass split by max bytes per part (record-safe). */
    public void splitByMaxBytes(Path input, Path outputDir, long maxBytesPerPart, Separator separator, IOConfig io)
            throws IOException {
        if (maxBytesPerPart <= 0) throw new IllegalArgumentException("maxBytesPerPart must be > 0");
        splitInternal(input, outputDir, maxBytesPerPart, separator, io);
    }


    /** 1-pass split by max bytes per part (record-safe) + contextual PartWriter (split+sort before writing). */
    public void splitByMaxBytes(Path input, Path outputDir, long maxBytesPerPart, Separator separator, IOConfig io, PartWriter partWriter)
            throws IOException {
        if (maxBytesPerPart <= 0) throw new IllegalArgumentException("maxBytesPerPart must be > 0");
        if (partWriter == null) throw new IllegalArgumentException("partWriter is required");
        splitInternalWithWriter(input, outputDir, maxBytesPerPart, separator, io, partWriter);
    }
    /** 1-pass split by number of parts (record-safe) + contextual PartWriter (split+sort before writing). */
    public void splitByParts(Path input, Path outputDir, int parts, Separator separator, IOConfig io, PartWriter partWriter)
            throws IOException {
        if (parts <= 0) throw new IllegalArgumentException("parts must be > 0");
        if (partWriter == null) throw new IllegalArgumentException("partWriter is required");
        long fileSize = Files.size(input);
        long target = Math.max(1, (fileSize + parts - 1L) / parts); // ceil
        splitInternalWithWriter(input, outputDir, target, separator, io, partWriter);
    }
    /**
     * Internal split method that emits lines (with detected endings) to a PartWriter.
     * Preserves CR/LF/CRLF/NONE endings and splits at line boundaries.
     */
    private void splitInternalWithWriter(Path input, Path outputDir, long targetBytes, Separator separator, IOConfig io, PartWriter partWriter)
            throws IOException {

        if (separator == null) throw new IllegalArgumentException("separator is required");
        if (!(separator instanceof NewlineSeparator)) {
            throw new IllegalArgumentException("PartWriter mode currently supports only NewlineSeparator");
        }
        if (io == null) io = IOConfig.defaults();

        Files.createDirectories(outputDir);

        // NOTE: We decode lines as UTF-8 because PartWriter accepts String. Ensure the writer uses the same charset.
        final java.nio.charset.Charset decodeCharset = java.nio.charset.StandardCharsets.UTF_8;
        int bufSize = Math.max(io.copyBufferBytes(), 256 * 1024);

        try (FileChannel in = FileChannel.open(input, READ)) {
            long fileSize = in.size();
            if (fileSize == 0) return;

            //ByteBuffer buf = ByteBuffer.allocateDirect(io.copyBufferBytes());
            ByteBuffer buf = ByteBuffer.allocate(bufSize); // heap buffer -> enables fast bulk appends
            int partIndex = 1;
            long partBytes = 0;
            boolean splitArmed = false;
            boolean wroteAnyLineInPart = false;

            // Accumulate bytes of the current line across buffers
            java.io.ByteArrayOutputStream lineBuf = new java.io.ByteArrayOutputStream(1024);

            // Only needed for CRLF spanning buffers
            boolean pendingCR = false;

            long pos = 0;
            //while (pos < fileSize) {
            while (true) {
                buf.clear();
                //int read = in.read(buf, pos);
                int read = in.read(buf);
                if (read <= 0) break;
                buf.flip();
                byte[] arr = buf.array();
                int lineStart = 0;

                for (int i = 0; i < read; i++) {
                    //byte b = buf.get(i);
                    byte b = arr[i];

                    // Resolve CR carried from previous buffer
                    if (pendingCR) {
                        if (b == (byte) '\n') {
                            long emitted = emitLineBytes(partWriter, lineBuf, LineEnding.CRLF, decodeCharset);
                            wroteAnyLineInPart = true;
                            partBytes += emitted;
                            pendingCR = false;
                            lineStart = i + 1; // skip '\n'
                            continue;
                        } else {
                            long emitted = emitLineBytes(partWriter, lineBuf, LineEnding.CR, decodeCharset);
                            wroteAnyLineInPart = true;
                            partBytes += emitted;
                            pendingCR = false;
                            // re-process current byte normally
                            //i--;
                            //continue;
                            // do NOT consume current byte; it belongs to the next line
                            lineStart = i;
                            // continue normal processing below (fall-through)
                        }
                    }
                    // case without CR carryover
                    if (b == (byte) '\n') {
                        // append bytes of the line in one shot
                        int len = i - lineStart;
                        if (len > 0) {
                            lineBuf.write(arr, lineStart, len);
                        }
                        long emitted = emitLineBytes(partWriter, lineBuf, LineEnding.LF, decodeCharset);
                        wroteAnyLineInPart = true;
                        partBytes += emitted;
                        lineStart = i + 1;

                    } else if (b == (byte) '\r') {
                        // append bytes of the line (excluding the CR)
                        int len = i - lineStart;
                        if (len > 0) {
                            lineBuf.write(arr, lineStart, len);
                        }

                        if (i + 1 < read) {
                            //byte next = buf.get(i + 1);
                            byte next = arr[i + 1];
                            if (next == (byte) '\n') {
                                long emitted = emitLineBytes(partWriter, lineBuf, LineEnding.CRLF, decodeCharset);
                                wroteAnyLineInPart = true;
                                partBytes += emitted;
                                i++; // consume '\n'
                                lineStart = i + 1;
                            } else {
                                long emitted = emitLineBytes(partWriter, lineBuf, LineEnding.CR, decodeCharset);
                                wroteAnyLineInPart = true;
                                partBytes += emitted;
                            }
                        } else {
                            // CR at end of buffer -> decide on next buffer
                            pendingCR = true;
                            lineStart = i + 1;
                        }
//                    } else {
//                        lineBuf.write(b);
                    }

                    // Arm split once we hit the target; split happens ONLY after a line ending
                    if (!splitArmed && partBytes >= targetBytes) {
                        splitArmed = true;
                    }

                    if (splitArmed && wroteAnyLineInPart) {
                        partWriter.endPart(partPath(outputDir, io, partIndex));
                        partIndex++;
                        partBytes = 0;
                        splitArmed = false;
                        wroteAnyLineInPart = false;
                    }
                }
                // Tail bytes: if the buffer ended mid-line (no line ending encountered),
                // append the remaining bytes so the line can be completed in the next buffer
                // or emitted at EOF with LineEnding.NONE.
                if (lineStart < read) {
                    lineBuf.write(arr, lineStart, read - lineStart);
                }
                pos += read;

            }

            // EOF: resolve pending CR
            if (pendingCR) {
                long emitted = emitLineBytes(partWriter, lineBuf, LineEnding.CR, decodeCharset);
                wroteAnyLineInPart = true;
                partBytes += emitted;
                pendingCR = false;
            } else if (lineBuf.size() > 0) {
                long emitted = emitLineBytes(partWriter, lineBuf, LineEnding.NONE, decodeCharset);
                wroteAnyLineInPart = true;
                partBytes += emitted;
            }

            if (wroteAnyLineInPart ) {
                partWriter.endPart(partPath(outputDir, io, partIndex));
            }
        }
    }



    private static Path partPath(Path outputDir, IOConfig io, int idx) {
        return outputDir.resolve(String.format(Locale.ROOT, "%s%04d%s", io.filePrefix(), idx, io.fileExtension()));
    }

    /** 1-pass split by number of parts (record-safe). */
    public void splitByParts(Path input, Path outputDir, int parts, Separator separator, IOConfig io)
            throws IOException {
        if (parts <= 0) throw new IllegalArgumentException("parts must be > 0");
        long fileSize = Files.size(input);
        long target = Math.max(1, fileSize / parts);
        splitInternal(input, outputDir, target, separator, io);
    }

    private void splitInternal(Path input, Path outputDir, long targetBytes, Separator separator, IOConfig io)
            throws IOException {

        if (separator == null) throw new IllegalArgumentException("separator is required");
        if (io == null) io = IOConfig.defaults();
        boolean isNewlineSep = separator instanceof NewlineSeparator;
        boolean isSingleByteSep = separator instanceof SingleByteSeparator;

        SingleByteSeparator sbs = null;
        if ( isSingleByteSep)
        {
             sbs = (SingleByteSeparator) separator;
        }
        Files.createDirectories(outputDir);

        try (FileChannel in = FileChannel.open(input, READ)) {
            long fileSize = in.size();
            if (fileSize == 0) return;

            ByteBuffer buf = ByteBuffer.allocateDirect(io.copyBufferBytes());

            int partIndex = 1;
            //FileChannel out = openPart(outputDir, io, partIndex);
            java.io.BufferedOutputStream out = openPart(outputDir, io, partIndex);

            long partBytes = 0;
            boolean splitArmed = false;

            // only needed for newline: CRLF spanning buffers
            boolean pendingCR = false;

            try {
                // cursor
                long pos = 0;
                while (pos < fileSize) {

                    buf.clear();
                    // read only buf byte from the pos
                    int read = in.read(buf, pos);
                    if (read <= 0) break;
                    // this set position of buffer to 0 and limit to read bytes, so we can iterate over [0..read) in the buffer
                    buf.flip();

                    // this is the position of the start of the current chunk within the buffer. We write out bytes [chunkStart..sepEnd) when we find a separator.
                    int chunkStart = 0;
                    // find the next separator in the buffer and write out chunks. If split is armed, we split immediately after the separator.
                    for (int i = 0; i < read; i++) {
                        byte b = buf.get(i);
                        int sepEnd = -1; // exclusive index within current buffer
                        // check if the current byte is a separator. If so, sepEnd is set to the index immediately after the separator (exclusive).
                        if (isSingleByteSep) {
                            if (b == sbs.getSep()) {
                                sepEnd = i + 1;
                            }
                        } else if (isNewlineSep) {
                            // First resolve CR carried from previous buffer
                            if (pendingCR) {
                                // this logic serve if ur read /n as first byte in current buffer, then the separator is CRLF across buffers. Otherwise, it was a lone CR.
                                if (b == (byte) '\n') {
                                    sepEnd = i + 1; // include '\n'
                                } else {
                                    sepEnd = 0; // lone CR ended at boundary between buffers
                                }
                                pendingCR = false;
                            }

                            if (sepEnd < 0) {
                                if (b == (byte) '\n') {
                                    sepEnd = i + 1;
                                } else if (b == (byte) '\r') {
                                    if (i + 1 < read) {
                                        byte next = buf.get(i + 1);
                                        if (next == (byte) '\n') {
                                            sepEnd = i + 2; // include CRLF
                                            i++; // consume '\n'
                                        } else {
                                            sepEnd = i + 1; // include CR
                                        }
                                    } else {
                                        // CR at end of buffer -> decide on next buffer
                                        pendingCR = true;
                                    }
                                }
                            }
                        } else {
                            throw new IllegalArgumentException("Unsupported separator implementation: " + separator.getClass());
                        }

                        if (sepEnd >= 0) {
                            // write bytes [chunkStart..sepEnd)
                            int len = sepEnd - chunkStart;
                            if (len > 0) {
                                writeSlice(out, buf, chunkStart, len);
                                partBytes += len;
                            }
                            chunkStart = sepEnd;

                            // arm split once we hit the target
                            if (!splitArmed && partBytes >= targetBytes) {
                                splitArmed = true;
                            }

                            // if split is armed, do it exactly after the separator
                            if (splitArmed) {
                                out.close();
                                partIndex++;
                                out = openPart(outputDir, io, partIndex);
                                partBytes = 0;
                                splitArmed = false;
                            }
                        }
                    }

                    // write remaining tail (after last separator in this buffer)
                    int tailLen = read - chunkStart;
                    if (tailLen > 0) {
                        writeSlice(out, buf, chunkStart, tailLen);
                        partBytes += tailLen;

                        if (!splitArmed && partBytes >= targetBytes) {
                            splitArmed = true;
                        }
                    }

                    pos += read;
                }

                // If file ends with pendingCR=true, we already wrote '\r' in the tail.
                // It acts as a separator at EOF; no special action needed.
            } finally {
                out.close();
            }
        }
    }



    /**
     * Open a new output channel for the given part index. The caller is responsible for closing the channel.
     * @param outputDir
     * @param io
     * @param idx
     * @return
     * @throws IOException
     */
    /*private static FileChannel openPart(Path outputDir, IOConfig io, int idx) throws IOException {
        // create file name with 4-digit zero-padded part index, e.g. "part-0001.txt"
        Path out = outputDir.resolve(String.format(Locale.ROOT, "%s%04d%s", io.filePrefix(), idx, io.fileExtension()));
        return FileChannel.open(out, WRITE, CREATE, TRUNCATE_EXISTING);
    }*/

    private static java.io.BufferedOutputStream openPart(Path outputDir, IOConfig io, int idx) throws IOException {
        Path out = outputDir.resolve(String.format(Locale.ROOT, "%s%04d%s", io.filePrefix(), idx, io.fileExtension()));
        return new java.io.BufferedOutputStream(
                Files.newOutputStream(out, WRITE, CREATE, TRUNCATE_EXISTING),
                1 << 20 // 1MB buffer
        );
    }

    /**
     * Write a slice of the source buffer to the output channel. This method handles partial writes by looping until the entire slice is written.
     * @param out
     * @param src
     * @param offset
     * @param len
     * @throws IOException
     */
    private static void writeSlice(FileChannel out, ByteBuffer src, int offset, int len) throws IOException {
        ByteBuffer dup = src.duplicate();
        dup.position(offset);
        dup.limit(offset + len);
        while (dup.hasRemaining()) {
            int written = out.write(dup);
            if (written <= 0) {
                throw new IOException("Failed to make progress while writing to file");
            }
        }
    }

    private static void writeSlice(java.io.OutputStream out, ByteBuffer src, int offset, int len) throws IOException {
        // Versione semplice: copia i len bytes dal ByteBuffer in un byte[]
        byte[] tmp = new byte[len];
        int oldPos = src.position();
        src.position(offset);
        src.get(tmp);
        src.position(oldPos);

        out.write(tmp);
    }

}