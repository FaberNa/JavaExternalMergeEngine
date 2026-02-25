package org.github.faberna.file.merge;

import org.github.faberna.file.merge.model.HeapItem;
import org.github.faberna.file.split.model.CustomBytesSeparator;
import org.github.faberna.file.split.model.NewlineSeparator;
import org.github.faberna.file.split.model.Separator;
import org.github.faberna.file.split.model.SingleByteSeparator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ChunkRecordReaderTest {

    @Test
    void heapItem_shouldStoreProvidedFields() {
        byte[] bytes = "abc".getBytes(StandardCharsets.UTF_8);
        HeapItem item = new HeapItem("line", bytes, 3, 42L);

        assertEquals("line", item.line);
        assertSame(bytes, item.recordBytes);
        assertEquals(3, item.chunkIndex);
        assertEquals(42L, item.seq);
    }
    static Stream<Arguments> heapOrderingCases() {
        Comparator<String> natural = Comparator.naturalOrder();
        return Stream.of(
                // line decides
                Arguments.of(new HeapItem("a", new byte[0], 0, 2), new HeapItem("b", new byte[0], 0, 1), natural, -1),
                Arguments.of(new HeapItem("b", new byte[0], 0, 2), new HeapItem("a", new byte[0], 0, 1), natural,  1),

                // same line -> seq decides
                Arguments.of(new HeapItem("x", new byte[0], 0, 1), new HeapItem("x", new byte[0], 0, 2), natural, -1),
                Arguments.of(new HeapItem("x", new byte[0], 0, 2), new HeapItem("x", new byte[0], 0, 1), natural,  1),
                Arguments.of(new HeapItem("x", new byte[0], 0, 7), new HeapItem("x", new byte[0], 0, 7), natural,  0)
        );
    }

    @ParameterizedTest
    @MethodSource("heapOrderingCases")
    void heapItemComparator_shouldSortByLineThenSeq(
            HeapItem a,
            HeapItem b,
            Comparator<String> lineComparator,
            int expectedSign
    ) {
        Comparator<HeapItem> heapComparator = (x, y) -> {
            int c = lineComparator.compare(x.line, y.line);
            if (c != 0) return c;
            return Long.compare(x.seq, y.seq);
        };

        assertEquals(Integer.signum(expectedSign), Integer.signum(heapComparator.compare(a, b)));

        // validate PQ behavior too
        PriorityQueue<HeapItem> pq = new PriorityQueue<>(heapComparator);
        pq.add(a);
        pq.add(b);

        HeapItem first = pq.poll();
        HeapItem second = pq.poll();

        assertNotNull(first);
        assertNotNull(second);
        assertNull(pq.poll());

        if (expectedSign < 0) {
            assertSame(a, first);
            assertSame(b, second);
        } else if (expectedSign > 0) {
            assertSame(b, first);
            assertSame(a, second);
        } else {
            assertTrue((first == a && second == b) || (first == b && second == a));
        }
    }

    static Stream<Arguments> separatorCases() {
        return Stream.of(
                Arguments.of("LF", new SingleByteSeparator((byte) '\n',1)),
                Arguments.of("US-0x1F", new SingleByteSeparator((byte) 0x1F,2)),
                Arguments.of("CRLF", new NewlineSeparator(2))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("separatorCases")
    void chunkRecordReader_shouldReadAllRecords_withoutSeparator(String name, Separator sep, @TempDir Path tmp) throws IOException {
        byte[] sepBytes = sep.bytes();

        // ["aa","b","","ccc"] + sep dopo ogni record
        byte[] data = concat(
                "aa".getBytes(StandardCharsets.UTF_8), sepBytes,
                "b".getBytes(StandardCharsets.UTF_8),  sepBytes,
                "".getBytes(StandardCharsets.UTF_8),   sepBytes,
                "ccc".getBytes(StandardCharsets.UTF_8),sepBytes
        );

        Path p = tmp.resolve("chunk.bin");
        Files.write(p, data);

        try (ChunkRecordReader r = ChunkRecordReader.open(p, sep)) {
            assertArrayEquals("aa".getBytes(StandardCharsets.UTF_8), r.nextRecord());
            assertArrayEquals("b".getBytes(StandardCharsets.UTF_8),  r.nextRecord());
            assertArrayEquals("".getBytes(StandardCharsets.UTF_8),   r.nextRecord());
            assertArrayEquals("ccc".getBytes(StandardCharsets.UTF_8),r.nextRecord());
            assertNull(r.nextRecord());
            assertNull(r.nextRecord()); // idempotent EOF
        }
    }

    @Test
    void chunkRecordReader_shouldReturnLastRecordEvenWithoutTrailingSeparator(@TempDir Path tmp) throws IOException {
        Separator sep = new SingleByteSeparator((byte) '\n',1);
        byte[] sepBytes = sep.bytes();

        byte[] data = concat(
                "aa".getBytes(StandardCharsets.UTF_8), sepBytes,
                "bb".getBytes(StandardCharsets.UTF_8) // no trailing sep
        );

        Path p = tmp.resolve("chunk-no-trailing-sep.txt");
        Files.write(p, data);

        try (ChunkRecordReader r = ChunkRecordReader.open(p, sep)) {
            assertArrayEquals("aa".getBytes(StandardCharsets.UTF_8), r.nextRecord());
            assertArrayEquals("bb".getBytes(StandardCharsets.UTF_8), r.nextRecord());
            assertNull(r.nextRecord());
        }
    }

    private static byte[] concat(byte[]... parts) {
        int len = 0;
        for (byte[] p : parts) len += p.length;
        byte[] out = new byte[len];
        int off = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, off, p.length);
            off += p.length;
        }
        return out;
    }

}