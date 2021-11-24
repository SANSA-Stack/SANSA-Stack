package net.sansa_stack.io.util;

import org.apache.commons.io.input.ProxyInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Workaround for HADOOP-17453: read(bts, off, len) with off != 0 is broken in several version of BZip2Codec
 * Invoking read with a non-zero offset creates a in intermediate buffer to which is read with a zero offset
 * The content of the intermidate buffer is then copied to the requesting buffer bts at the appropriate offset.
 */
public class InputStreamWithZeroOffsetRead
    extends ProxyInputStream
{
    /**
     * Constructs a new ProxyInputStream.
     *
     * @param proxy the InputStream to delegate to
     */
    public InputStreamWithZeroOffsetRead(InputStream proxy) {
        super(proxy);
    }

    @Override
    public int read(byte[] bts, int off, int len) throws IOException {
        int result;
        if (off == 0) {
            result = super.read(bts, off, len);
        } else {
            byte[] buffer = new byte[len];
            result = super.read(buffer, 0, len);
            if (result > 0) { // Copy if there is at least one byte
                System.arraycopy(buffer, 0, bts, off, result);
            }
        }

        return result;
    }
}
