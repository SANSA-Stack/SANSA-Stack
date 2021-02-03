package net.sansa_stack.rdf.common;

import com.google.common.primitives.Ints;
import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/** ReadabalByteChannel whose read method is guaranteed to return at a specific position
 *  (before crossing it) such as a split boundary
 */
public class InterruptingReadableByteChannel
    implements ReadableByteChannel, SeekableDecorator
{
    protected byte[] buffer = new byte[1024 * 1024];
    protected boolean interrupted = false;
    protected InputStream in;
    protected Seekable seekable;
    protected long interruptPos;

    public InterruptingReadableByteChannel(
            InputStream in,
            Seekable seekable,
            long interruptPos) {
        this.in = in;
        this.seekable = seekable;
        this.interruptPos = interruptPos;
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        long pos = seekable.getPos();

        int remainingUntilInterrupt = !interrupted && pos < interruptPos
            ? Ints.saturatedCast(interruptPos - pos)
                : Integer.MAX_VALUE;

        int toRead = Math.min(byteBuffer.remaining(), remainingUntilInterrupt);
        toRead = Math.min(toRead, buffer.length);

        int contrib = in.read(buffer, 0, toRead);

        if (contrib >= 0) {
            byteBuffer.put(buffer, 0, contrib);
        }

        if (pos == interruptPos) {
            interrupted = true;
        }

        return contrib;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public Seekable getSeekable() {
        return seekable;
    }
}
