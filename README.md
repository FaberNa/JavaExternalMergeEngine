[![codecov](https://codecov.io/github/FaberNa/JavaExternalMergeEngine/graph/badge.svg?token=04S3NFM40S)](https://codecov.io/github/FaberNa/JavaExternalMergeEngine)

# JavaExternalMergeEngine

## SortedSplitEngine

`SortedSplitEngine` is the component that creates **sorted runs** during the split phase of the
external sort pipeline.

Instead of only splitting a large file into smaller record-safe parts, it also sorts each part in
memory before the part is written to disk.

This makes it the natural first step of a classic external sort workflow:

1. split the input into sorted runs
2. merge the sorted runs into one globally ordered output

The ordering of each line is defined by a `KeySpec`, so the same logical key can later be reused by
merge logic as well.

Important notes:

- sorting is **local to each part**
- global ordering is achieved only after the merge phase
- original line endings are preserved while writing parts

Typical usage:

```java
KeySpec keySpec = KeySpec.of(
        Segment.range(0, 10),
        Segment.afterDelimiter('|', 0, null)
);

SortedSplitEngine engine = new SortedSplitEngine(
        new SplitEngine(),
        keySpec,
        keySpec.comparator()
);
engine.splitByMaxBytes(input, outputDir, 128 * 1024 * 1024, Separator.LF, ioConfig);

```
### Comparator behavior

The default comparator works as follows:

1. Iterate through segments in declaration order
2. Delegate comparison to each segment
3. Return the first non-zero result
4. If all segments are equal → result is `0`

This allows building multi-column style comparisons similar to SQL `ORDER BY col1, col2`.



Example:

```java
var spec = KeySpec.of(
    Segment.range(0, 3),
    Segment.afterDelimiter('|', 0, 2)
);

Comparator<String> cmp = spec.comparator();
```

## KeySpec: the sort key model

`KeySpec` represents the logical key used to order lines during the external sort pipeline.

It is built from one or more `Segment` definitions. Each segment describes how to read and compare
one portion of a line, so a `KeySpec` can model simple keys as well as composite keys.

This same abstraction is reused in both main phases of the pipeline:

- **Split phase**: each chunk is sorted in memory using the `KeySpec` comparator.
- **Merge phase**: sorted chunk files are merged using the same `KeySpec`, ensuring consistent
  global ordering.

This is important because external sorting is only correct if the ordering logic is identical in
both phases. `KeySpec` provides that single source of truth.

In practice, you can think of it like a SQL `ORDER BY` clause for text lines:

- one segment = one sort criterion
- multiple segments = lexicographic comparison across multiple criteria

The default `KeySpec.comparator()` is optimized for performance and avoids materializing keys. When
needed, `extractKey(...)` can still be used for debugging or custom comparator integration.



---

# Segment Abstraction

```java
public sealed interface Segment
```

A `Segment` defines one portion of the logical sort key and provides:

- `compare(String a, String b)` → zero-allocation comparison
- `appendKey(String line, StringBuilder out)` → materialized key support

Two concrete implementations are currently supported:

- `RangeSegment`
- `DelimitedSegment`

---

## RangeSegment

```java
Segment.range(int startInclusive, int endExclusive)
```

Extracts and compares a fixed character range.

Example:

```java
Segment.range(0, 3)
```

On line:

```
"ABCDEFG"
```

The segment represents:

```
"ABC"
```

Characteristics:

- No substring allocation during comparison
- Safe for shorter lines (bounds handled internally)
- Ideal for fixed-width file formats

---

## DelimitedSegment

```java
Segment.afterDelimiter(char delimiter, int occurrenceIndex, int lengthAfter)
```

Extracts characters immediately after the N-th occurrence of a delimiter.

Example (fixed length):

```java
Segment.afterDelimiter('|', 1, 3)
```

For line:

```
"a|bbb|cc|dddd"
```

The 2nd `|` (index = 1) is after `bbb`.

Extracted portion (first 3 chars after the delimiter):

```
"cc|"
```

Example (until next delimiter):

```java
Segment.afterDelimiter('|', 1, null)
```

For the same line:

```
"a|bbb|cc|dddd"
```

Extracted portion (all characters until the next `|`, or end-of-line if none):

```
"cc"
```

Notes:
- `occurrenceIndex` is 0-based.
- If the requested delimiter occurrence is not found, the segment is empty.

---

# Design Principles

The design follows these principles:

- Zero-allocation comparison where possible
- Clear separation between comparison logic and key materialization
- Composability (multiple segments in sequence)
- Predictable behavior in edge cases (short lines, missing delimiters)

This model enables flexible, high-performance external sorting strategies over very large files.





How to for test 

gawk 'BEGIN{
payload="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
for(i=1;i<=10000000;i++){
printf "%010d %s\n", i, payload
}
}' > test_1gb.txt