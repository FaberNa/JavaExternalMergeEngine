package org.github.faberna.file.split.model;

import java.nio.channels.FileChannel;

public final class CustomBytesSeparator implements Separator {
    int call=0;

    @Override
    public byte[] bytes() {
        return new byte[]{}; // usa il nome reale del campo
    }
    @Override
    public long findNextSeparatorEnd(FileChannel ch, long from, long fileSize) {
        call++;
        if (call == 1) {
            return 0; // boundary <= last (last iniziale è 0)
        } else {
            return fileSize; // retry valido
        }
    }
}
