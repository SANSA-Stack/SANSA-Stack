package net.sansa_stack.io.util;

import org.apache.commons.io.input.ProxyInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * A wrapper whose close() method is a noop.
 * In contrast to CloseShieldInputStream this class' read method
 * will not indicate EOF after close.
 */
public class InputStreamWithCloseIgnore
    extends ProxyInputStream
{
    /**
     * Constructs a new ProxyInputStream.
     *
     * @param proxy the InputStream to delegate to
     */
    public InputStreamWithCloseIgnore(InputStream proxy) {
        super(proxy);
    }

    @Override
    public void close() throws IOException {
        // System.err.println("CLOSE PREVENTED");
        // Do nothing
    }
}
