package org.github.faberna.file.split.plan;

import org.github.faberna.file.split.model.Range;
import org.github.faberna.file.split.model.Separator;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public final class SplitPlanner {


    public SplitPlan planByMaxBytes(Path input, Path outDir, long maxBytes, Separator sep) throws IOException {
        try (FileChannel ch = FileChannel.open(input, StandardOpenOption.READ)) {
            long size = ch.size();
            long target = maxBytes;
            return new SplitPlan(input, outDir, computeRanges(ch, size, target, sep));
        }
    }

    public SplitPlan planByParts(Path input, Path outDir, int parts, Separator sep) throws IOException {
        try (FileChannel ch = FileChannel.open(input, StandardOpenOption.READ)) {
            long size = ch.size();
            long target = Math.max(1, size / parts);
            return new SplitPlan(input, outDir, computeRanges(ch, size, target, sep));
        }
    }

    private List<Range> computeRanges(FileChannel ch, long fileSize, long targetChunk, Separator sep) throws IOException {
        List<Long> boundaries = new ArrayList<>();
        boundaries.add(0L);

        long nextTarget = targetChunk;

        while (nextTarget < fileSize) {
            long boundary = sep.findNextSeparatorEnd(ch, nextTarget, fileSize);
            if (boundary < 0) break;

            long last = boundaries.getLast();
            if (boundary <= last) {
                long retryFrom = Math.min(fileSize, last + 1);
                long retry = sep.findNextSeparatorEnd(ch, retryFrom, fileSize);
                if (retry < 0 || retry <= last) break;
                boundary = retry;
            }

            boundaries.add(boundary);
            nextTarget = boundary + targetChunk;
        }

        // Ensure the final boundary is exactly EOF, but avoid duplicating it
        long lastBoundary = boundaries.getLast();
        if (lastBoundary != fileSize) {
            boundaries.add(fileSize);
        }

        List<Range> ranges = new ArrayList<>(boundaries.size() - 1);
        for (int i = 0; i < boundaries.size() - 1; i++) {
            ranges.add(new Range(boundaries.get(i), boundaries.get(i + 1)));
        }
        return ranges;
    }
}