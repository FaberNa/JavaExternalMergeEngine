package org.github.faberna.file.split.model;

import java.io.IOException;
import java.nio.channels.FileChannel;

public sealed interface Separator permits NewlineSeparator, SingleByteSeparator, CustomBytesSeparator {
    /** Ritorna offset subito DOPO il separatore trovato da 'from' in poi, oppure -1 se non c’è fino a EOF. */
    long findNextSeparatorEnd(FileChannel ch, long from, long fileSize) throws IOException;
}