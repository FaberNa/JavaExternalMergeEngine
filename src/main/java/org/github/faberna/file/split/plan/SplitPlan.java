package org.github.faberna.file.split.plan;


import org.github.faberna.file.split.model.Range;

import java.nio.file.Path;
import java.util.List;

public record SplitPlan(Path input, Path outputDir, List<Range> parts) {}
