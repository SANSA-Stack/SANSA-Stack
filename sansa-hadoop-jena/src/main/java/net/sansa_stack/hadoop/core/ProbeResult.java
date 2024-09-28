package net.sansa_stack.hadoop.core;

import java.time.Duration;

class ProbeResult {
    protected long candidatePos;
    protected long probeCount;
    protected Duration totalDuration;

    public ProbeResult(long candidatePos, long probeCount, Duration totalDuration) {
        super();
        this.candidatePos = candidatePos;
        this.probeCount = probeCount;
        this.totalDuration = totalDuration;
    }

    public long candidatePos() {
        return candidatePos;
    }

    public long probeCount() {
        return probeCount;
    }

    public Duration totalDuration() {
        return totalDuration;
    }

    @Override
    public String toString() {
        return "ProbeResult [candidatePos=" + candidatePos + ", probeCount=" + probeCount + ", totalDuration="
                + totalDuration + "]";
    }
}