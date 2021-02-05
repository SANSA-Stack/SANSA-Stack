package net.sansa_stack.rdf.common;

import org.apache.commons.io.input.ProxyInputStream;

import java.io.IOException;
import java.io.InputStream;

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
