package net.sansa_stack.hadoop;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/** This class is intended for TRIG and all sub syntaxes (nquads, turtle, ...)
 * It pads each line with a number of white spaces followed by a hash.
 *
 * <pre>
 * # Example      #
 * :s :p :o       #
 * :x :y :z       #
 * </pre>
 */
public class OutputStreamRightPad
    extends OutputStream
//         extends WritableByteChannelWrapperBase
{
    protected OutputStream delegate;

    int BUFFER_SIZE_INCREMENT = 4 * 1024;
    int amount;


    protected byte[] byteArray = new byte[BUFFER_SIZE_INCREMENT];
    protected int pos = 0;
    protected int lineno = 1;

    public OutputStreamRightPad(OutputStream delegate, int amount) {
        // super(delegate);
        super();
        this.delegate = delegate;
        this.amount = amount;
    }

    private void appendBytes(byte[] bs, int offset, int len) {
        for (int i = offset; i < offset + len; ++i) {
            appendByte(bs[i]);
        }
    }

        private void appendByte(byte b) {
        if (pos > byteArray.length) {
            byte[] tmp = new byte[byteArray.length + BUFFER_SIZE_INCREMENT];
            System.arraycopy(byteArray, 0, tmp, 0, tmp.length);
            byteArray = tmp;
        }
        byteArray[pos] = b;
        ++pos;
    }

    @Override
    public void write(int bb) throws IOException {
        byte b = (byte)bb;
        if (b == '\n') {
            // TODO Could append line to each line
            int labelPadSize = 10;
            String label = Integer.toString(lineno);
            byte[] labelBytes = label.getBytes(StandardCharsets.UTF_8);
            int labelPadAmount = labelPadSize - labelBytes.length;

            // data             [wsAmount] #   _   [labelPadSize]_123  _   #    \n
            int wsAmount = amount - (pos + 1 + 1 + labelPadSize      + 1 + 1  +  1);
            for (int i = 0; i < wsAmount; ++i) {
                appendByte((byte)' ');
            }

            appendByte((byte)'#');
            appendByte((byte)' ');

            for (int i = 0; i < labelPadAmount; ++i) {
                appendByte((byte)' ');
            }
            appendBytes(labelBytes, 0, labelBytes.length);
            appendByte((byte)' ');
            appendByte((byte)'#');
            appendByte(b);
            flush();
            ++lineno;
        } else {
            appendByte(b);
        }
    }

    private void flushLine() throws IOException {
        getDelegate().write(byteArray, 0, pos);
        pos = 0;
    }
    @Override
    public void flush() throws IOException {
        flushLine();
        getDelegate().flush();
    }

    @Override
    public void close() throws IOException {
        getDelegate().close();
    }

    public OutputStream getDelegate() {
        return delegate;
    }

    public static OutputStreamRightPad rightPad(OutputStream out, int amount) {
        return new OutputStreamRightPad(out, amount);
    }
}
