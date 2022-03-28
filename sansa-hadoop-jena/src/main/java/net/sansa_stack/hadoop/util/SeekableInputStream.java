package net.sansa_stack.hadoop.util;

import org.apache.commons.io.input.ProxyInputStream;
import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.io.InputStream;

/**
 * A basic wrapper that combines Hadoop's Seekable and InputStream into one class.
 * Because InputStream is not an interface we have to work which such a wrapper class
 * if we want the methods of both combined.
 *
 */
public class SeekableInputStream
    extends ProxyInputStream
    implements SeekableDecorator
{
    protected Seekable seekable;

    public <T> SeekableInputStream(InputStream in) {
        this(in, (Seekable)in);
    }

    /**
     * Constructs a new ProxyInputStream.
     *
     */
    public SeekableInputStream(InputStream in, Seekable seekable) {
        super(in);
        this.seekable = seekable;
    }

    /**
     * You should not change the position of the underlying seekable directly while this
     * input stream is in use. Conversely, only use this class' seek methods for changing the
     * position. Otherwise it my result in an inconsistent state.
     *
     * @return The underlying seekable.
     */
    @Override
    public Seekable getSeekable() {
        return seekable;
    }
}
