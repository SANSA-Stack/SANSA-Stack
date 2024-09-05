package net.sansa_stack.hadoop.util;

import org.aksw.commons.io.util.channel.ReadableByteChannelFromInputStream;
import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class SeekableByteChannelFromSeekableInputStream
    extends ReadableByteChannelFromInputStream
    implements SeekableByteChannel
{
    protected Seekable seekable;

    public SeekableByteChannelFromSeekableInputStream(InputStream in) {
        this(in, (Seekable)in);
    }

    public SeekableByteChannelFromSeekableInputStream(InputStream in, Seekable seekable) {
        super(in);
        this.seekable = seekable;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position() throws IOException {
        return seekable.getPos();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        seekable.seek(newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        return -1; // Some of the InputStream wrapper call available() which eventually calls size() - so we must not raise an exception here
        // throw new UnsupportedOperationException();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
        return false;
    }
}
