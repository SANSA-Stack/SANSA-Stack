package net.sansa_stack.rdf.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.function.Predicate;

/**
 * Readable byte channel wrapper that before every read checks for an
 * end-of-file (eof) condition.
 * Once true, any subsequent read immediatly returns -1 (eof).
 * Used to prevent reading across hadoop split boundaries
 */
public class ReadableByteChannelWithConditionalBound<T extends ReadableByteChannel>
        extends ReadableByteChannelDecoratorBase<T>
{
    protected Predicate<? super T> testForEof;
    protected boolean isInEofState = false;

    public ReadableByteChannelWithConditionalBound(
            T delegate,
            Predicate<? super T> testForEof) {
        super(delegate);
        this.testForEof = testForEof;
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        isInEofState = isInEofState || testForEof.test(getDelegate());

        int result;
        if (isInEofState) {
            result = -1;
        } else {
            result = getDelegate().read(byteBuffer);
        }

        return result;
    }
}
