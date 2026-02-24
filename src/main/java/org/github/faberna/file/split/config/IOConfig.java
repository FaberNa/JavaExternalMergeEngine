package org.github.faberna.file.split.config;


public record IOConfig(
        int copyBufferBytes,
        int parallelism, // number of threads for parallel splitting
        boolean preferSequential,
        String filePrefix,
        String fileExtension
) {
    public IOConfig {

        if (copyBufferBytes <= 0) {
            throw new IllegalArgumentException("copyBufferBytes must be positive");
        }
        if (!preferSequential && parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be positive");
        }
        if (filePrefix == null || filePrefix.isBlank()) {
            throw new IllegalArgumentException("filePrefix is required");
        }
        if (fileExtension == null) {
            throw new IllegalArgumentException("fileExtension is required");
        }
    }

    public static IOConfig defaults() {
        return new IOConfig(
                64 * 1024, // standard IO buffer size for file copying
                Runtime.getRuntime().availableProcessors(),
                false,
                "part-",
                ".txt"
        );
    }
}

