package net.sansa_stack.nio.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public interface ReadableByteChannelDecorator
        extends ReadableByteChannel
{
    ReadableByteChannel getDelegate();

    @Override
    default boolean isOpen() {
        return getDelegate().isOpen();
    }

    @Override
    default void close() throws IOException {
        getDelegate().close();
    }

    @Override
    default int read(ByteBuffer byteBuffer) throws IOException {
        return getDelegate().read(byteBuffer);
    }
}
