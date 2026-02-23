package org.github.faberna.file.split.model;
/**
 * Newline type detected in the input.
 *
 * NOTE: We keep CR only for completeness (old Mac files). Most real-world inputs are LF or CRLF.
 */
public enum LineEnding {
    NONE(""), // last row without newline chars (e.g. last line of file)
    LF("\n"),
    CRLF("\r\n"),
    CR("\r");

    private final String text;

    LineEnding(String text) {
        this.text = text;
    }

    public String text() {
        return text;
    }
}