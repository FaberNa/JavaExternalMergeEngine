package org.github.faberna.file.segment.model;

public  interface Segment<T> {

    /**
     *  Compare the key segments of a and b, returning a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
     * @param a first line to compare
     * @param b second line to compare
     * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second
     */
    int compare(T a, T b);

    /**
     * Append the key segment of the line to the output StringBuilder.
     * Method used to build the output it's used only for debug purposes, as it allocates a new String for the key segment.
     * For actual merging and comparison, the compare method is used, which does not allocate.
     * @param value the line to extract the key segment from
     * @param out the StringBuilder to append the key segment to
     */
    void appendKey(T value, StringBuilder out);

    static Segment<String> range(int startInclusive, int endExclusive) {
        return new RangeSegment(startInclusive, endExclusive, Mode.LEX);
    }

}