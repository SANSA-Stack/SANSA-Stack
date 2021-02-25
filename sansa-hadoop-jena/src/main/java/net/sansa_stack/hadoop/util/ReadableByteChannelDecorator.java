package net.sansa_stack.hadoop.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public interface ReadableByteChannelDecorator<T extends ReadableByteChannel>
        extends ReadableByteChannel
{
     T getDelegate();

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
