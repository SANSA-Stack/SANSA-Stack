package net.sansa_stack.hadoop.format.jena.base;

import net.sansa_stack.hadoop.core.pattern.CustomPattern;

public class RecordReaderConf {
    /**
     * The maximum number of elements to parse during probing.
     * Elements are aggregated into records. For example, RDF quads aggregated into Datasets.
     * If the elements are already the records, then probeElementCountKey is ignored and only probeRecordCountKey is considered.
     */
    protected String probeElementCountKey;
    protected String minRecordLengthKey;
    protected String maxRecordLengthKey;
    protected String probeRecordCountKey;
    protected CustomPattern recordSearchPattern;

    public String getProbeElementCountKey() { return probeElementCountKey; }

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
            String probeElementCountKey,
            String minRecordLengthKey,
            String maxRecordLengthKey,
            String probeRecordCountKey,
            CustomPattern recordSearchPattern) {
        this.probeElementCountKey = probeElementCountKey;
        this.minRecordLengthKey = minRecordLengthKey;
        this.maxRecordLengthKey = maxRecordLengthKey;
        this.probeRecordCountKey = probeRecordCountKey;
        this.recordSearchPattern = recordSearchPattern;
    }
}
