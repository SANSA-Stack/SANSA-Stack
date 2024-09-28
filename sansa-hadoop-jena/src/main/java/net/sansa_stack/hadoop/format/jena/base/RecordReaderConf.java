package net.sansa_stack.hadoop.format.jena.base;

import net.sansa_stack.hadoop.core.pattern.CustomPattern;

public class RecordReaderConf {
    protected String minRecordLengthKey;
    protected String maxRecordLengthKey;
    protected String probeRecordCountKey;
    protected CustomPattern recordSearchPattern;

    public String getMinRecordLengthKey() {
        return minRecordLengthKey;
    }

    public String getMaxRecordLengthKey() {
        return maxRecordLengthKey;
    }

    public String getProbeRecordCountKey() {
        return probeRecordCountKey;
    }

    public CustomPattern getRecordSearchPattern() {
        return recordSearchPattern;
    }

    public RecordReaderConf(
            String minRecordLengthKey,
            String maxRecordLengthKey,
            String probeRecordCountKey,
            CustomPattern recordSearchPattern) {
        this.minRecordLengthKey = minRecordLengthKey;
        this.maxRecordLengthKey = maxRecordLengthKey;
        this.probeRecordCountKey = probeRecordCountKey;
        this.recordSearchPattern = recordSearchPattern;
    }
}
