package net.sansa_stack.hadoop.core;

import com.google.common.collect.Range;

public class OffsetSeekResult {
    protected boolean success;

    // Applicable if success is false
    protected long startPos;
    protected long endPos;
    // Range<Long> readRange;

    public OffsetSeekResult(boolean success, long startPos, long endPos) {
        this.success = success;
        this.startPos = startPos;
        this.endPos = endPos;
    }

    public boolean isSuccess() {
        return success;
    }

    public long getStartPos() {
        return startPos;
    }

    public long getEndPos() {
        return endPos;
    }

    long getLength() {
        return endPos - startPos;
    }
}
