package org.github.faberna.file.split.sorter;

import org.github.faberna.file.split.model.LineEnding;
import org.github.faberna.file.segment.model.KeySpec;
import org.github.faberna.file.split.model.LineRecord;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Buffers an entire part in memory, sorts it using KeySpec, then writes it once.
 *
 * This is the "contextual" split+sort behavior:
 * - buffer lines for the current part
 * - sort in memory
 * - write the part exactly once (no read-back)
 *
 * Newline preservation:
 * - each line is written back with the SAME newline style detected in input.
 */
public final class InMemorySortingPartWriter implements PartWriter {

    private final KeySpec keySpec;
    private final Comparator<String> keyComparator;

    private final Charset charset;

    private final List<LineRecord> buffer = new ArrayList<>();


    public Charset getCharset() {
        return charset;
    }

    public InMemorySortingPartWriter(KeySpec keySpec, Comparator<String> keyComparator, Charset charset) {
        this.keySpec = Objects.requireNonNull(keySpec, "keySpec is required");
        this.keyComparator = Objects.requireNonNull(keyComparator, "keyComparator is required");
        this.charset = Objects.requireNonNull(charset, "charset is required");
    }

    /**
     * Buffer the line and its original line ending for later sorting and writing.
     * @param line
     * @param ending
     */
    @Override
    public void acceptLine(String line, LineEnding ending) {
        buffer.add(new LineRecord(line, ending));
    }

    /**
     * Sorts the buffered lines by their content (line text only, not including newline chars) using KeySpec,
     *  when we find the part we capp
     * @param partFile
     * @throws IOException
     */
    @Override
    public void endPart(Path partFile) throws IOException {
        Objects.requireNonNull(partFile, "partFile is required");

        // Build a comparator for whole lines using KeySpec.
        // KeySpec is composed of Segments; its comparator delegates comparisons to those segments.
        // If you want a custom ordering over the materialized key, use comparator(keyComparator).
        Comparator<String> lineComparator = keySpec.comparator(keyComparator);

        // Sort records by their line content (ending does not affect ordering)
        buffer.sort((r1, r2) -> lineComparator.compare(r1.line(), r2.line()));

        // When the original input has no final newline, we may have ONE record with LineEnding.NONE.
        // After sorting, that record might not be last anymore; if we write it as NONE in the middle,
        // the next line will be concatenated. To keep records separated, we only allow NONE for the
        // last record written in this part.
        LineEnding defaultEnding = buffer.stream()
                .map(LineRecord::ending)
                .filter(e -> e != LineEnding.NONE)
                .findFirst()
                .orElse(LineEnding.LF);

        // Write once to temp then atomic replace
        Path tmp = partFile.resolveSibling(partFile.getFileName().toString() + ".tmp");
        try (BufferedWriter w = Files.newBufferedWriter(tmp, charset)) {
            for (int i = 0; i < buffer.size(); i++) {
                LineRecord r = buffer.get(i);
                w.write(r.line());

                boolean last = (i == buffer.size() - 1);
                LineEnding ending = r.ending();

                // If NONE is not on the last record, write a real line separator to avoid concatenation.
                if (ending == LineEnding.NONE && !last) {
                    ending = defaultEnding;
                }
                if (last && ending == defaultEnding) {
                    ending = LineEnding.NONE;
                }
                w.write(ending.text());
            }
        }

        try {
            Files.move(tmp, partFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            // Ready for next part (also ensures we don't keep memory if an exception occurs)
            buffer.clear();
        }
    }
}
