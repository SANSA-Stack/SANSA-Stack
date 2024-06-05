package net.sansa_stack.hadoop.core;

import org.aksw.commons.io.buffer.array.BufferOverReadableChannel;
import org.aksw.commons.io.input.SeekableReadableChannel;

import java.util.function.BiFunction;

/**
 * Tests whether parsing a certain amount of records from the channel at a given offset returns successfully.
 * If it fails, the byte position of the error is returned.
 *
 * @param <U>
 */
public interface Prober<U>
    // Arg1: The input byte channel
    // Arg2: A buffer from which the read records can be obtained.
    extends BiFunction<SeekableReadableChannel<byte[]>, BufferOverReadableChannel<U[]>, OffsetSeekResult>
{
}
