package net.sansa_stack.hadoop.core;

import org.aksw.commons.io.buffer.array.BufferOverReadableChannel;
import org.aksw.commons.io.input.ReadableChannel;
import org.aksw.commons.io.input.SeekableReadableChannel;

public class ProbeResult<U> {
    protected SeekableReadableChannel<byte[]> inChannel;
    protected int successfulProbeEltCount;

    protected BufferOverReadableChannel<byte[]> byteBuffer;
    protected ReadableChannel<byte[]> byteChannel;

    protected BufferOverReadableChannel<U[]> eltBuffer;
    protected ReadableChannel<U[]> eltChannel;
}
