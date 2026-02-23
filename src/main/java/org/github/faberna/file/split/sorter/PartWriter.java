package org.github.faberna.file.split.sorter;

import org.github.faberna.file.split.model.LineEnding;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Receives parsed lines and writes a part file.
 *
 * IMPORTANT: We preserve the original newline style per line (LF/CRLF/CR).
 */
public interface PartWriter {

    /**
     * Accept one logical line WITHOUT newline characters, plus its original line ending.
     */
    void acceptLine(String line, LineEnding ending);

    /**
     * Finalize current part and write it to {@code partFile}.
     * Implementations may sort and/or buffer content.
     */
    void endPart(Path partFile) throws IOException;
}





