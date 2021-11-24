package net.sansa_stack.nio.util;

import com.google.common.primitives.Ints;
import net.sansa_stack.nio.util.SeekableByteChannelDecoratorBase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class InterruptingSeekableByteChannel
    extends SeekableByteChannelDecoratorBase
{
    protected long interruptPos;

    public InterruptingSeekableByteChannel(SeekableByteChannel decoratee, long interruptPos) {
        super(decoratee);
        this.interruptPos = interruptPos;
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        long pos = position();

        int remainingUntilInterrupt = pos < interruptPos
                ? Ints.saturatedCast(interruptPos - pos)
                : Integer.MAX_VALUE;

        int capacity = byteBuffer.remaining();

        int toRead = remainingUntilInterrupt == 0
            ? capacity
            : Math.min(capacity, remainingUntilInterrupt);

        if (toRead != capacity) {
            byteBuffer = byteBuffer.duplicate();
            byteBuffer.limit(byteBuffer.position() + toRead);
        }

        int result = super.read(byteBuffer);
        return result;
    }
}
