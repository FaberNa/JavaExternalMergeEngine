package org.github.faberna.file.split.util;

import org.github.faberna.file.split.SplitUtil;
import org.github.faberna.file.split.model.LineEnding;
import org.github.faberna.file.split.sorter.PartWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SplitUtilTest {
    // -------------------------------------------------
    // SplitUtil.emitLineBytes tests
    // -------------------------------------------------

    static Stream<Arguments> emitLineBytesCases() {
        return Stream.of(
                Arguments.of(LineEnding.LF, 1),
                Arguments.of(LineEnding.CR, 1),
                Arguments.of(LineEnding.CRLF, 2),
                Arguments.of(LineEnding.NONE, 0)
        );
    }

    @ParameterizedTest
    @MethodSource("emitLineBytesCases")
    void emitLineBytes_shouldReturnOriginalBytesConsumed_andResetBuffer(LineEnding ending, int endingBytes) {
        PartWriter writer = mock(PartWriter.class);
        ByteArrayOutputStream lineBuf = new ByteArrayOutputStream();

        // include multi-byte UTF-8 chars to ensure we count ORIGINAL BYTES (lineBuf.size), not chars
        String line = "Aé€"; // 'é' = 2 bytes in UTF-8, '€' = 3 bytes in UTF-8
        Charset charset = StandardCharsets.UTF_8;
        byte[] lineBytes = line.getBytes(charset);
        lineBuf.writeBytes(lineBytes);

        long consumed = SplitUtil.emitLineBytes(writer, lineBuf, ending, charset);

        assertEquals(lineBytes.length + endingBytes, consumed);
        assertEquals(0, lineBuf.size(), "lineBuf must be reset after emitting");

        verify(writer, times(1)).acceptLine(line, ending);
        verifyNoMoreInteractions(writer);
    }
    @Test
    void emitLineBytes_shouldHandleEmptyLine() {
        PartWriter writer = mock(PartWriter.class);
        ByteArrayOutputStream lineBuf = new ByteArrayOutputStream();
        long consumed = SplitUtil.emitLineBytes(writer, lineBuf, LineEnding.LF, StandardCharsets.UTF_8);
        assertEquals(1L, consumed);
        assertEquals(0, lineBuf.size());
        verify(writer).acceptLine("", LineEnding.LF);
        verifyNoMoreInteractions(writer);
    }



}
