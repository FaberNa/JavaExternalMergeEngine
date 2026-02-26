package org.github.faberna.file.split.plan;

import org.github.faberna.file.split.model.CustomBytesSeparator;
import org.github.faberna.file.split.model.NewlineSeparator;
import org.github.faberna.file.split.model.Range;
import org.github.faberna.file.split.model.Separator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class SplitPlannerPlanByPartsTest {

    @TempDir
    Path tmp;

    static Stream<Arguments> cases() {
        // Using 10 lines => fileSize = 10 * 4 = 40 bytes
        // Boundaries must align on newline ends (multiples of 4).
        return Stream.of(
                // maxBytes >= fileSize => one range [0,40)
                Arguments.of(10, 100L, List.of(0L, 40L)),
                Arguments.of(10, 40L,  List.of(0L, 40L)),

                // maxBytes=10 => target=10 -> next newline end after 10 is 12
                // then 12+10=22 -> next end 24; 24+10=34 -> next end 36; then EOF 40
                Arguments.of(10, 10L,  List.of(0L, 12L, 24L, 36L, 40L)),

                // maxBytes=5 => nextTarget 5 -> boundary 8; nextTarget 13 -> boundary 16; 21->24; 29->32; 37->40; EOF already hit
                Arguments.of(10, 5L,   List.of(0L, 8L, 16L, 24L, 32L, 40L)),

                // maxBytes=1 => nextTarget 1 -> boundary 4; then 5->8; 9->12; ... -> every line
                Arguments.of(10, 1L,   List.of(0L, 4L, 8L, 12L, 16L, 20L, 24L, 28L, 32L, 36L, 40L)),

                // maxBytes=0 => nextTarget 0 -> boundary 4; then 4->8; ... also every line (still terminates)
                Arguments.of(10, 0L,   List.of(0L, 4L, 8L, 12L, 16L, 20L, 24L, 28L, 32L, 36L, 40L))
        );
    }

    @ParameterizedTest
    @MethodSource("cases")
    void shouldPlanExpectedBoundaries_ForFixedLineFile(int lines, long maxBytes, List<Long> expectedBoundaries) throws Exception {
        Path input = createTestFile(lines);
        Path outDir = tmp.resolve("out-maxbytes");
        Files.createDirectories(outDir);

        SplitPlanner planner = new SplitPlanner();
        NewlineSeparator sep = new NewlineSeparator(8, null); // buffer small but ok

        SplitPlan plan = planner.planByMaxBytes(input, outDir, maxBytes, sep);
        List<Range> parts = plan.parts();

        assertEquals(expectedBoundaries.size() - 1, parts.size(), "Unexpected number of parts");

        for (int i = 0; i < parts.size(); i++) {
            Range r = parts.get(i);
            assertEquals(expectedBoundaries.get(i), r.startInclusive(), "start mismatch at part " + i);
            assertEquals(expectedBoundaries.get(i + 1), r.endExclusive(), "end mismatch at part " + i);
        }

        // invariants: contiguous and full coverage
        long fileSize = Files.size(input);
        assertEquals(0L, parts.getFirst().startInclusive());
        assertEquals(fileSize, parts.getLast().endExclusive());

        for (int i = 1; i < parts.size(); i++) {
            assertEquals(parts.get(i - 1).endExclusive(), parts.get(i).startInclusive(),
                    "parts must be contiguous at index " + i);
        }

        // internal boundaries land on newline end
        String content = Files.readString(input, StandardCharsets.UTF_8);
        for (int i = 0; i < parts.size() - 1; i++) {
            long end = parts.get(i).endExclusive();
            assertTrue(end > 0, "endExclusive must be > 0 for internal boundaries");
            assertEquals('\n', content.charAt((int) end - 1), "boundary not on newline at part " + i);
        }
    }

    @Test
    void shouldThrowIOExceptionWhenInputMissing() {
        Path missing = tmp.resolve("missing.txt");
        Path outDir = tmp.resolve("out-missing");
        NewlineSeparator sep = new NewlineSeparator(8, null);

        SplitPlanner planner = new SplitPlanner();
        assertThrows(IOException.class, () -> planner.planByMaxBytes(missing, outDir, 10L, sep));
    }

    private Path createTestFile(int lines) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lines; i++) {
            sb.append(String.format("%03d%n", i));
        }
        Path file = tmp.resolve("input.txt");
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
        return file;
    }

    @Test
    void shouldSplitIntoExactPartsAndAlignToNewlines() throws Exception {
        Path input = createTestFile(10); // 10 lines
        Path out = tmp.resolve("out");
        Files.createDirectories(out);

        NewlineSeparator sep = new NewlineSeparator(8, null);
        SplitPlanner planner = new SplitPlanner();

        SplitPlan plan = planner.planByParts(input, out, 3, sep);

        List<Range> ranges = plan.parts();

        // Deve creare 3 range
        assertEquals(3, ranges.size());

        // Primo range parte da 0
        assertEquals(0, ranges.getFirst().startInclusive());

        // Ultimo range termina alla dimensione file
        long fileSize = Files.size(input);
        assertEquals(fileSize, ranges.getLast().endExclusive());

        // Verifica che siano contigui
        for (int i = 1; i < ranges.size(); i++) {
            assertEquals(
                    ranges.get(i - 1).endExclusive(),
                    ranges.get(i).startInclusive()
            );
        }

        // Verifica che ogni boundary interno sia su newline
        String content = Files.readString(input);
        for (int i = 0; i < ranges.size() - 1; i++) {
            long end = ranges.get(i).endExclusive();
            assertEquals('\n', content.charAt((int) end - 1));
        }
    }

    @Test
    void shouldReturnSingleRangeWhenPartsIsOne() throws Exception {
        Path input = createTestFile(5);
        Path out = tmp.resolve("out2");
        Files.createDirectories(out);

        SplitPlanner planner = new SplitPlanner();
        NewlineSeparator sep = new NewlineSeparator(16, null);

        SplitPlan plan = planner.planByParts(input, out, 1, sep);

        assertEquals(1, plan.parts().size());

        Range r = plan.parts().getFirst();
        assertEquals(0, r.startInclusive());
        assertEquals(Files.size(input), r.endExclusive());
    }

    @Test
    void shouldThrowArithmeticExceptionWhenPartsIsZero() throws Exception {
        Path input = createTestFile(3);
        Path out = tmp.resolve("out3");
        Files.createDirectories(out);

        SplitPlanner planner = new SplitPlanner();
        NewlineSeparator sep = new NewlineSeparator(8, null);

        assertThrows(ArithmeticException.class,
                () -> planner.planByParts(input, out, 0, sep));
    }

    @Test
    void shouldHandleMorePartsThanLinesGracefully() throws Exception {
        Path input = createTestFile(3);
        Path out = tmp.resolve("out4");
        Files.createDirectories(out);

        SplitPlanner planner = new SplitPlanner();
        NewlineSeparator sep = new NewlineSeparator(8, null);

        SplitPlan plan = planner.planByParts(input, out, 10, sep);

        // Non può creare più range delle righe reali
        assertTrue(plan.parts().size() <= 3);
    }

    @Test
    void shouldRetryWhenBoundaryIsNotGreaterThanLast() throws Exception {
        Path input = createTestFile(5);
        Path out = tmp.resolve("retryTest");
        Files.createDirectories(out);

        SplitPlanner planner = new SplitPlanner();

        // Separator finto che forza boundary <= last alla prima chiamata
        Separator sep = new CustomBytesSeparator();

        SplitPlan plan = planner.planByParts(input, out, 2, sep);

        List<Range> ranges = plan.parts();

        // Deve comunque creare un range valido
        assertEquals(1, ranges.size());
        assertEquals(0, ranges.getFirst().startInclusive());
        assertEquals(Files.size(input), ranges.getFirst().endExclusive());
    }
}