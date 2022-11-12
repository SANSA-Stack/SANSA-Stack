package net.sansa_stack.hadoop.util;

import org.aksw.commons.util.stack_trace.StackTraceUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;


/**
 * A wrapper for hadoop input streams created from codecs in ReadMode.BY_BLOCK:
 * Defers reading by one byte such that position changes are advertised on the byte BEFORE
 * the block boundary rather than on the byte AFTER it.
 */
public class DeferredSeekablePushbackInputStream
    extends PushbackInputStream
    implements SeekableDecorator
{
    protected Seekable seekable;


    // Fallback buffer in case buffers provided to read have a capacity of less than 2:
    // The minimum internal read length is 2: At least the first byte will be
    // read and the second one pushed back for the next read.
    // protected byte[] fallbackBuffer = new byte[2];

    private static final int TRANSFER_SIZE = 8192 + 1; // Add 1 byte for look ahead

    /**
     * Unsafe reads modify the byte after the reported number of read bytes
     * in the read buffer.
     *
     * Example: If buf := [a, b, c, d] and "read(buf)" returns 3,
     * then buf[3] may no longer hold a 'd'.
     *
     * As this may cause data corruption, safe read mode uses an intermediate buffer
     * with additional copying to prevent changing data outside of the reported range.
     */
    protected ReadMode readMode = ReadMode.BUFFERED;

    private enum ReadMode {
        UNSAFE, // First reads into the given buffer directly but then unreads the last byte and then reports one fewer byte read.
                // This is unsafe because the buffer is changed outside of the reported range (but within the allowed range)
        BUFFERED, // Read the requested amount + 1 into a temp buffer. Limited by TRANSFER_SIZE.
    }

    protected byte fallbackBuffer[] = new byte[TRANSFER_SIZE];

    public DeferredSeekablePushbackInputStream(InputStream in) {
        this(in, (Seekable)in);
    }

    public DeferredSeekablePushbackInputStream(InputStream in, Seekable seekable) {
        super(in, 1);
        this.seekable = seekable;
    }

    @Override
    public Seekable getSeekable() {
        return seekable;
    }


    @Override
    public int read() throws IOException {
        int result = read(fallbackBuffer, 0, 2);
        if (result > 0) {
            result = fallbackBuffer[0];
        }

        return result;
    }

    /**
     * This method essentially delays reads by one byte.
     * Whereas hadoop's codec protocol reads one byte past block boundaries,
     * this method buffers one byte on every read such that once the underlying
     * stream reads accross the block boundary -- indicated by a position change --
     * only the byte before the block boundary is returned.
     *
     * @return
     * @throws IOException
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        byte[] out;
        int o, l;

        // Whether this read call reads into a buffer first
        boolean isBufferedRead;
        if (len == 0) {
            return 0; // Corner case - exit immediately
        } else if (len == 1) {
            out = fallbackBuffer;
            o = 0;
            l = 2;
            isBufferedRead = true;
        } else { // n > 1
            switch (readMode) {
            case BUFFERED:
                int bytesToRead = Math.min(len + 1, TRANSFER_SIZE);
                out = fallbackBuffer;
                o = 0;
                l = bytesToRead;
                isBufferedRead = true;
                break;
            case UNSAFE:
                out = b;
                o = off;
                l = len;
                isBufferedRead = false;
                break;
            default:
                throw new IllegalStateException("Unknown read mode: " + readMode);
            }
        }

        int n = 0;
        while (n == 0) {
            // Zero byte reads are invalid by the input stream protocol
            // so we need to ensure we don't expose such:
            // Zero byte reads occurr internally when reading starts just before
            // a block boundary: In that case one byte is read past the bounday,
            // however because it is the last and only byte of a read it is pushed back.
            // This leaves no further bytes that can be returned to the client
            // Therefore invoke read once more.
            n = readInternal(out, o, l);
        }

        if (n > 0 && isBufferedRead) {
            System.arraycopy(fallbackBuffer, 0, b, off, n);
        }

        return n;
    }

    @Override
    public synchronized void close() throws IOException {
        // System.out.println("Closed: " + ObjectUtils.identityToString(this ) + " - " + StackTraceUtils.toString(Thread.currentThread().getStackTrace()));
        super.close();
    }

    /** This method is assumed to be invoked with len >= 2 */
    protected int readInternal(byte[] b, int off, int len) throws IOException {
        // ensureOpen() is private
        if (in == null) {
            throw new IOException("Stream closed");
        }

        int result;

        // Available amount of data in the pushback buffer
        int avail = buf.length - pos;
        int n = super.read(b, off, len);

        // If content was only served from the buffer
        // then the delegate stream has reached its end
        // Note: len is at least 2 and avail at most 1.
        // Therefore len > avail always holds - i.e. the underlying buffer
        // will always receive a request
        if (n == avail || n < 0) {
            // eof encountered - yield read data without pushing
            // anything back such that the next read will return eof
            result = n;
        } else { // if (n > 1) {
            // Report one less byte than actually read
            result = n - 1;

            // Unread the last byte from this read so we have
            // at least one byte for the next read
            byte lastByte = b[result];
            unread(lastByte);
        }
        return result;
    }

    protected void afterSeek() {
        // Effectively clear the pushback buffer
        this.pos = this.buf.length;
    }

    @Override
    public void seek(long l) throws IOException {
        seekable.seek(l);
        afterSeek();
    }

    @Override
    public long getPos() throws IOException {
        return seekable.getPos();
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        boolean result = seekable.seekToNewSource(l);
        afterSeek();
        return result;
    }
}
