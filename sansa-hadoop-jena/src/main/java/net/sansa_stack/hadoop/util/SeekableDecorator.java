package net.sansa_stack.hadoop.util;

import org.apache.hadoop.fs.Seekable;

import java.io.IOException;

public interface SeekableDecorator
    extends Seekable
{
    Seekable getSeekable();

    @Override
    default void seek(long l) throws IOException {
        getSeekable().seek(l);
    }

    @Override
    default long getPos() throws IOException {
        return getSeekable().getPos();
    }

    @Override
    default boolean seekToNewSource(long l) throws IOException {
        return getSeekable().seekToNewSource(l);
    }
}
