package net.sansa_stack.nio.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.function.Predicate;

/**
 * Readable byte channel wrapper that before every read checks for an
 * end-of-file (eof) condition.
 * Once true, any subsequent read immediately returns -1 (eof).
 * Used to prevent reading across hadoop split boundaries
 */
public class ReadableByteChannelWithConditionalBound<T extends ReadableByteChannel>
        extends ReadableByteChannelDecoratorBase<T>
{
    protected Predicate<? super ReadableByteChannelWithConditionalBound<T>> testForEof;
    protected boolean isInEofState = false;
    protected long bytesRead = 0;

    public ReadableByteChannelWithConditionalBound(
            T delegate,
            Predicate<? super ReadableByteChannelWithConditionalBound<T>> testForEof) {
        super(delegate);
        this.testForEof = testForEof;
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        isInEofState = isInEofState || testForEof.test(this);

        int result;
        if (isInEofState) {
            result = -1;
        } else {
            result = getDelegate().read(byteBuffer);
            if (result >= 0) {
                bytesRead += result;
            } else {
                isInEofState = true;
            }
        }

        return result;
    }

    public long getBytesRead() {
        return bytesRead;
    }
}
