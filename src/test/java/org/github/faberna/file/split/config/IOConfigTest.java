package org.github.faberna.file.split.config;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IOConfigTest {

    @Test
    void defaults_ShouldReturnExpectedContractValues() {
        IOConfig cfg = IOConfig.defaults();

        assertEquals(64 * 1024, cfg.copyBufferBytes());
        assertEquals(Runtime.getRuntime().availableProcessors(), cfg.parallelism());
        assertFalse(cfg.preferSequential());
        assertEquals("part-", cfg.filePrefix());
        assertEquals(".txt", cfg.fileExtension());

        // extra sanity (optional)
        assertTrue(cfg.parallelism() > 0);
    }
    @Test
    void shouldCreateConfig_WhenAllValuesAreValid() {
        IOConfig config = new IOConfig(
                64 * 1024,
                4,
                false,
                "part-",
                ".txt"
        );

        assertEquals(64 * 1024, config.copyBufferBytes());
        assertEquals(4, config.parallelism());
        assertFalse(config.preferSequential());
        assertEquals("part-", config.filePrefix());
        assertEquals(".txt", config.fileExtension());
    }

    @Test
    void shouldThrow_WhenCopyBufferIsZero() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IOConfig(0, 4, false, "part-", ".txt")
        );

        assertEquals("copyBufferBytes must be positive", ex.getMessage());
    }

    @Test
    void shouldThrow_WhenCopyBufferIsNegative() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new IOConfig(-1, 4, false, "part-", ".txt")
        );
    }

    @Test
    void shouldThrow_WhenParallelismIsZero_AndPreferSequentialIsFalse() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new IOConfig(1024, 0, false, "part-", ".txt")
        );

        assertEquals("parallelism must be positive", ex.getMessage());
    }

    @Test
    void shouldNotThrow_WhenParallelismIsZero_ButPreferSequentialIsTrue() {
        assertDoesNotThrow(() ->
                new IOConfig(1024, 0, true, "part-", ".txt")
        );
    }

    @Test
    void shouldThrow_WhenFilePrefixIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new IOConfig(1024, 4, false, null, ".txt")
        );
    }

    @Test
    void shouldThrow_WhenFilePrefixIsBlank() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new IOConfig(1024, 4, false, "   ", ".txt")
        );
    }

    @Test
    void shouldThrow_WhenFileExtensionIsNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new IOConfig(1024, 4, false, "part-", null)
        );
    }
}