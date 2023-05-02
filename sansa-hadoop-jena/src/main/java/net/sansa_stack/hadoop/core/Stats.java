package net.sansa_stack.hadoop.core;

import java.io.Serializable;

public class Stats
    implements Serializable
{
    private static final long serialVersionUID = 1L;

    protected long splitStart;
    protected long splitEnd;

//    protected long regionStart;
//    protected Duration regionStartSearchTime;

    protected Boolean regionStartSearchReadOverSplitEnd;

    /** If the search read over the region end it means that the parser had
     * to be restarted with the detected region start and end offsets.
     * This attribute can only be true if regionStartSearchReadOverSplitEnd is also true.
     */
    protected Boolean regionStartSearchReadOverRegionEnd;

    protected ProbeResult regionStartProbeResult;
    protected ProbeResult regionEndProbeResult;

    protected long totalElementCount;
    protected long tailElementCount;

    protected long totalRecordCount;

    protected Long totalBytesRead;

//    protected long regionEnd;
//    protected Duration regionEndTime;

    // Number probing attempts
//    protected long headProbeCount;
//    protected long tailProbeCount;

    // protected List<Long> probePosSample;

    public long getSplitStart() {
        return splitStart;
    }

    public Stats setSplitStart(long splitStart) {
        this.splitStart = splitStart;
        return this;
    }

    public long getSplitEnd() {
        return splitEnd;
    }

    public Stats setSplitEnd(long splitEnd) {
        this.splitEnd = splitEnd;
        return this;
    }

//    public long getRegionStart() {
//        return regionStart;
//    }
//
//    public Stats setRegionStart(long regionStart) {
//        this.regionStart = regionStart;
//        return this;
//    }
//
//    public Duration getRegionStartSearchTime() {
//        return regionStartSearchTime;
//    }
//
//    public Stats setRegionStartSearchTime(Duration regionStartSearchTime) {
//        this.regionStartSearchTime = regionStartSearchTime;
//        return this;
//    }

    public boolean isRegionStartSearchReadOverSplitEnd() {
        return regionStartSearchReadOverSplitEnd;
    }

    public Stats setRegionStartSearchReadOverSplitEnd(Boolean regionStartSearchReadOverSplitEnd) {
        this.regionStartSearchReadOverSplitEnd = regionStartSearchReadOverSplitEnd;
        return this;
    }

    public Boolean isRegionStartSearchReadOverRegionEnd() {
        return regionStartSearchReadOverRegionEnd;
    }

    public Stats setRegionStartSearchReadOverRegionEnd(Boolean regionStartSearchReadOverRegionEnd) {
        this.regionStartSearchReadOverRegionEnd = regionStartSearchReadOverRegionEnd;
        return this;
    }

    public long getTotalElementCount() {
        return totalElementCount;
    }

    public Stats setTotalElementCount(long totalElementCount) {
        this.totalElementCount = totalElementCount;
        return this;
    }

    public long getTailElementCount() {
        return tailElementCount;
    }

    public Stats setTailElementCount(long tailElementCount) {
        this.tailElementCount = tailElementCount;
        return this;
    }

    public long getRecordCount() {
        return totalRecordCount;
    }

    public Stats setRecordCount(long recordCount) {
        this.totalRecordCount = recordCount;
        return this;
    }

//    public long getRegionEnd() {
//        return regionEnd;
//    }
//
//    public Stats setRegionEnd(long regionEnd) {
//        this.regionEnd = regionEnd;
//        return this;
//    }
//
//    public Duration getRegionEndTime() {
//        return regionEndTime;
//    }
//
//    public Stats setRegionEndTime(Duration regionEndTime) {
//        this.regionEndTime = regionEndTime;
//        return this;
//    }

    public long getTotalRecordCount() {
        return totalRecordCount;
    }

    public Stats setTotalRecordCount(long totalRecordCount) {
        this.totalRecordCount = totalRecordCount;
        return this;
    }

    public ProbeResult getRegionStartProbeResult() {
        return regionStartProbeResult;
    }

    public Stats setRegionStartProbeResult(ProbeResult regionStartProbeResult) {
        this.regionStartProbeResult = regionStartProbeResult;
        return this;
    }

    public ProbeResult getRegionEndProbeResult() {
        return regionEndProbeResult;
    }

    public Stats setRegionEndProbeResult(ProbeResult regionEndProbeResult) {
        this.regionEndProbeResult = regionEndProbeResult;
        return this;
    }

    public Long getTotalBytesRead() {
        return totalBytesRead;
    }

    public Stats setTotalBytesRead(Long totalBytesRead) {
        this.totalBytesRead = totalBytesRead;
        return this;
    }

    @Override
    public String toString() {
        return "Stats [splitStart=" + splitStart + ", splitEnd=" + splitEnd + ", regionStartSearchReadOverSplitEnd="
                + regionStartSearchReadOverSplitEnd + ", regionStartSearchReadOverRegionEnd="
                + regionStartSearchReadOverRegionEnd + ", regionStartProbeResult=" + regionStartProbeResult
                + ", regionEndProbeResult=" + regionEndProbeResult + ", totalElementCount=" + totalElementCount
                + ", tailElementCount=" + tailElementCount
                + ", totalRecordCount=" + totalRecordCount + ", totalBytesRead=" + totalBytesRead + "]";
    }
}
