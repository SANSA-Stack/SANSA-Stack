package net.sansa_stack.spark.cli.util;

import java.util.List;

public interface RdfOutputConfig {
    String getOutputFormat();
    String getPartitionFolder();
    String getTargetFile();
    Long getPrefixOutputDeferCount();
    List<String> getPrefixSources();
    boolean isOverwriteAllowed();
}
