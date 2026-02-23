[![codecov](https://codecov.io/gh/FaberNa/JavaExternalMergeEngine/graph/badge.svg?token=JMZAIXDO1O)](https://codecov.io/gh/FaberNa/JavaExternalMergeEngine)
# JavaExternalMergeEngine
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

---

# Segment Abstraction

```java
public sealed interface Segment
```

A `Segment` defines:

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