package org.github.faberna.file.split.model;

import java.util.Objects;

/**
 * One line (without newline chars) + its original line ending.
 */
public record LineRecord(String line, LineEnding ending) {
    public LineRecord {
        Objects.requireNonNull(line, "line is required");
        Objects.requireNonNull(ending, "ending is required");
    }
}