package org.github.faberna.file.merge.model;

/**
 * Heap element: decoded line for comparison + raw record bytes for output.
 */
public final class HeapItem {
    public final String line;
    public final byte[] recordBytes;
    public final int chunkIndex;
    public final long seq;

    public HeapItem(String line, byte[] recordBytes, int chunkIndex, long seq) {
        this.line = line;
        this.recordBytes = recordBytes;
        this.chunkIndex = chunkIndex;
        this.seq = seq;
    }
}