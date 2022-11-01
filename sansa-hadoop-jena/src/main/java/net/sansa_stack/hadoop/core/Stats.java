package net.sansa_stack.hadoop.core;

import java.io.Serializable;
import java.time.Duration;

public class Stats
    implements Serializable
{
    private static final long serialVersionUID = 1L;

    protected long splitStart;
    protected long splitEnd;

    protected long regionStart;
    protected Duration regionStartSearchTime;

    protected boolean regionStartSearchReadOverSplitEnd;

    /** If the search read over the region end it means that the parser had
     * to be restarted with the detected region start and end offsets.
     * This attribute can only be true if regionStartSearchReadOverSplitEnd is also true.
     */
    protected boolean regionStartSearchReadOverRegionEnd;

    protected long totalElementCount;
    protected long tailElementCount;
    protected long tailRecordCount; // should be 1 at most

    protected long totalRecordCount;

    protected long regionEnd;
    protected Duration regionEndTime;

    // Number probing attempts
    protected long headProbeCount;
    protected long tailProbeCount;

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

    public long getRegionStart() {
        return regionStart;
    }

    public Stats setRegionStart(long regionStart) {
        this.regionStart = regionStart;
        return this;
    }

    public Duration getRegionStartSearchTime() {
        return regionStartSearchTime;
    }

    public Stats setRegionStartSearchTime(Duration regionStartSearchTime) {
        this.regionStartSearchTime = regionStartSearchTime;
        return this;
    }

    public boolean isRegionStartSearchReadOverSplitEnd() {
        return regionStartSearchReadOverSplitEnd;
    }

    public Stats setRegionStartSearchReadOverSplitEnd(boolean regionStartSearchReadOverSplitEnd) {
        this.regionStartSearchReadOverSplitEnd = regionStartSearchReadOverSplitEnd;
        return this;
    }

    public boolean isRegionStartSearchReadOverRegionEnd() {
        return regionStartSearchReadOverRegionEnd;
    }

    public Stats setRegionStartSearchReadOverRegionEnd(boolean regionStartSearchReadOverRegionEnd) {
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

    public long getRegionEnd() {
        return regionEnd;
    }

    public Stats setRegionEnd(long regionEnd) {
        this.regionEnd = regionEnd;
        return this;
    }

    public Duration getRegionEndTime() {
        return regionEndTime;
    }

    public Stats setRegionEndTime(Duration regionEndTime) {
        this.regionEndTime = regionEndTime;
        return this;
    }

    public long getTailRecordCount() {
        return tailRecordCount;
    }

    public void setTailRecordCount(long tailRecordCount) {
        this.tailRecordCount = tailRecordCount;
    }

    public long getTotalRecordCount() {
        return totalRecordCount;
    }

    public void setTotalRecordCount(long totalRecordCount) {
        this.totalRecordCount = totalRecordCount;
    }

    public long getHeadProbeCount() {
        return headProbeCount;
    }

    public Stats setHeadProbeCount(long headProbeCount) {
        this.headProbeCount = headProbeCount;
        return this;
    }

    public long getTailProbeCount() {
        return tailProbeCount;
    }

    public Stats setTailProbeCount(long tailProbeCount) {
        this.tailProbeCount = tailProbeCount;
        return this;
    }

    @Override
    public String toString() {
        return "Stats [splitStart=" + splitStart + ", splitEnd=" + splitEnd + ", regionStart=" + regionStart
                + ", regionStartSearchTime=" + regionStartSearchTime + ", regionStartSearchReadOverSplitEnd="
                + regionStartSearchReadOverSplitEnd + ", regionStartSearchReadOverRegionEnd="
                + regionStartSearchReadOverRegionEnd + ", totalElementCount=" + totalElementCount
                + ", tailElementCount=" + tailElementCount + ", tailRecordCount=" + tailRecordCount
                + ", totalRecordCount=" + totalRecordCount + ", regionEnd=" + regionEnd + ", regionEndTime="
                + regionEndTime + ", headProbeCount=" + headProbeCount + ", tailProbeCount=" + tailProbeCount + "]";
    }

}
