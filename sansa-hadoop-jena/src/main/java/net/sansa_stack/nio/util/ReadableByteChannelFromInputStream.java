package net.sansa_stack.nio.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;

/**
 * A replacement for Channels.newChannel with the following changes for
 * interoperability with hadoop and our GenericRecordReader:
 * - Does not close the underlying stream on interrupt
 * - This implementation's read method just delegates to the input stream
 *   (without additional buffering / repeated reads).
 *   This way hadoop's posititon advertising remains usable.
 */
public class ReadableByteChannelFromInputStream
        implements ReadableByteChannel {

    private static final int TRANSFER_SIZE = 8192;
    protected InputStream in;
    protected byte buf[] = new byte[0];
    protected boolean open = true;

    public ReadableByteChannelFromInputStream(InputStream in) {
        this.in = in;
    }

    public int read(ByteBuffer dst) throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        int result;
        int len = dst.remaining();

        if (dst.hasArray()) {
            int off = dst.position();
            byte[] arr = dst.array();
            result = in.read(arr, off, len);
        } else {
            int bytesToRead = Math.min(len, TRANSFER_SIZE);
            if (buf.length < bytesToRead) {
                buf = new byte[bytesToRead];
            }

            result = in.read(buf, 0, len);

            if (result > 0) {
                dst.put(buf, 0, result);
            }
        }

        return result;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() throws IOException {
        in.close();
        open = false;
    }
}
