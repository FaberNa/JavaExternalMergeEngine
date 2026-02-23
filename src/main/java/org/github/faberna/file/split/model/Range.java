package org.github.faberna.file.split.model;

public record Range(long startInclusive, long endExclusive) {
    public long length() { return endExclusive - startInclusive; }
}