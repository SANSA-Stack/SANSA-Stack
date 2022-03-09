package net.sansa_stack.spark.cli.impl;

import java.util.List;

public interface RdfOutputConfig {
    String getOutputFormat();
    String getPartitionFolder();
    String getTargetFile();
    Long getPrefixOutputDeferCount();
    List<String> getPrefixSources();
}
