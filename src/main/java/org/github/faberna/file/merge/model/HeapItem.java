package org.github.faberna.file.merge.model;

public final class HeapItem {
    final String line;          // current record
    final int chunkIndex;       // which chunk
    final long seq;             // tie-breaker for stability (optional)

    HeapItem(String line, int chunkIndex, long seq) {
        this.line = line;
        this.chunkIndex = chunkIndex;
        this.seq = seq;
    }
}