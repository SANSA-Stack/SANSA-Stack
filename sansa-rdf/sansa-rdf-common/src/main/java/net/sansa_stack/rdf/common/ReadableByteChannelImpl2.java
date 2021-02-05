package net.sansa_stack.rdf.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

// ReadableByteChannelImpl2 without the Interruptible stuff
public class ReadableByteChannelImpl2
        implements ReadableByteChannel {
    InputStream in;
    private static final int TRANSFER_SIZE = 8192;
    private byte buf[] = new byte[0];
    private boolean open = true;

    public ReadableByteChannelImpl2(InputStream in) {
        this.in = in;
    }

    public int read(ByteBuffer dst) throws IOException {
        int len = dst.remaining();
        int totalRead = 0;
        int bytesRead = 0;
        while (totalRead < len) {
            int bytesToRead = Math.min((len - totalRead),
                    TRANSFER_SIZE);
            if (buf.length < bytesToRead)
                buf = new byte[bytesToRead];
            if ((totalRead > 0) && !(in.available() > 0))
                break; // block at most once
            bytesRead = in.read(buf, 0, bytesToRead);
            if (bytesRead < 0)
                break;
            else
                totalRead += bytesRead;
            dst.put(buf, 0, bytesRead);
        }
        if ((bytesRead < 0) && (totalRead == 0))
            return -1;

        return totalRead;
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
