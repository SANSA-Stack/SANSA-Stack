package net.sansa_stack.hadoop.util;

import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

public class SeekablePushbackInputStream
        extends PushbackInputStream
        implements SeekableDecorator
{
    protected Seekable seekable;

    public SeekablePushbackInputStream(InputStream in, int size) {
        this(in, (Seekable)in, size);
    }

    public SeekablePushbackInputStream(InputStream in, Seekable seekable, int size) {
        super(in, size);
        this.seekable = seekable;
    }

    @Override
    public Seekable getSeekable() {
        return seekable;
    }

    protected void afterSeek() {
        // Effectively clear the pushback buffer
        this.pos = this.buf.length;
    }

    @Override
    public void seek(long l) throws IOException {
        getSeekable().seek(l);
        afterSeek();
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        boolean result = getSeekable().seekToNewSource(l);
        afterSeek();
        return result;
    }
}
