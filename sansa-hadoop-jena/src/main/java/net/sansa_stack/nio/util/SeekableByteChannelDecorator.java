package net.sansa_stack.nio.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public interface SeekableByteChannelDecorator
    extends SeekableByteChannel
{
    SeekableByteChannel getDecoratee();

    @Override
    default SeekableByteChannel position(long newPosition) throws IOException {
        getDecoratee().position(newPosition);
        return this;
    }

    @Override
    default long position() throws IOException {
        return getDecoratee().position();
    }

    @Override
    default void close() throws IOException {
        getDecoratee().close();
    }

    @Override
    default int read(ByteBuffer dst) throws IOException {
        return getDecoratee().read(dst);
    }

    @Override
    default int write(ByteBuffer src) throws IOException {
        return getDecoratee().write(src);
    }

    @Override
    default boolean isOpen() {
        return getDecoratee().isOpen();
    }

    @Override
    default long size() throws IOException{
        return getDecoratee().size();
    }

    @Override
    default SeekableByteChannel truncate(long size) throws IOException {
        getDecoratee().truncate(size);
        return this;
    }
}
