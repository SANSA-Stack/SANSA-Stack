package net.sansa_stack.hadoop.core;

import java.util.function.Function;

public class TailBufferChannel
{
    public static Function<Long, Long> splitIdFn(long splitOffset, long splitLength) {
        return offset -> (offset - splitOffset) / splitLength;
    }
}
