package net.sansa_stack.hadoop.util;

import com.google.common.primitives.Ints;
import org.apache.hadoop.fs.Seekable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

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
    protected long bytesRead = 0;
    protected Consumer<InterruptingReadableByteChannel> interruptPosFoundCallback;


    public InterruptingReadableByteChannel(
            InputStream in,
            Seekable seekable,
            long interruptPos) {
        this(in, seekable, interruptPos, null);
    }

    public InterruptingReadableByteChannel(
            InputStream in,
            Seekable seekable,
            long interruptPos,
            Consumer<InterruptingReadableByteChannel> interruptPosFoundCallback) {
        this.in = in;
        this.seekable = seekable;
        this.interruptPos = interruptPos;
        this.interruptPosFoundCallback = interruptPosFoundCallback;
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
            bytesRead += contrib;
            byteBuffer.put(buffer, 0, contrib);
        }

        if (pos == interruptPos) {
            interrupted = true;
            if (interruptPosFoundCallback != null) {
                interruptPosFoundCallback.accept(this);
            }
        }

        return contrib;
    }

    public long getBytesRead() {
        return bytesRead;
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
