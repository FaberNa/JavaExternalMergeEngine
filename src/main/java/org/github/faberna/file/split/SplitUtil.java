package org.github.faberna.file.split;

import org.github.faberna.file.split.model.LineEnding;
import org.github.faberna.file.split.sorter.PartWriter;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;

public class SplitUtil {

    private SplitUtil() {
        /* This utility class should not be instantiated */
    }

    public static void emitLine(PartWriter writer, ByteArrayOutputStream lineBuf, LineEnding ending, Charset charset) {
        String line = lineBuf.toString(charset);
        writer.acceptLine(line, ending);
        lineBuf.reset();
    }

    /** Emits one line and returns the number of ORIGINAL bytes consumed (line bytes + ending bytes). */
    public static long emitLineBytes(PartWriter partWriter,
                                      java.io.ByteArrayOutputStream lineBuf,
                                      LineEnding ending,
                                      java.nio.charset.Charset charset) {
        int lineSize = lineBuf.size();
        SplitUtil.emitLine(partWriter, lineBuf, ending, charset);
        int endingBytes = switch (ending) {
            case CRLF -> 2;
            case CR, LF -> 1;
            case NONE -> 0;
        };
        return (long) lineSize + (long) endingBytes;
    }

}



