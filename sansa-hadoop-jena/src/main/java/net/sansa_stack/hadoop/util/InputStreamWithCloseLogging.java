package net.sansa_stack.hadoop.util;

import org.apache.commons.io.input.ProxyInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/** Util class to debug a stream already closed exception */
public class InputStreamWithCloseLogging
    extends ProxyInputStream
{
    protected Throwable creationStackTrace;
    protected BiConsumer<? super Throwable, ? super Throwable> stackTraceConsumer;

    public InputStreamWithCloseLogging(InputStream proxy, BiConsumer<? super Throwable, ? super Throwable> stackTraceConsumer) {
        super(proxy);
        this.creationStackTrace = new Throwable();
        this.stackTraceConsumer = stackTraceConsumer;
    }

    @Override
    public void close() throws IOException {
        Throwable closingStackTrace = new Throwable();
        stackTraceConsumer.accept(creationStackTrace, closingStackTrace);
        super.close();
    }

    /** Convenience method for e.g. .wrap(inputStream, ExceptionUtils::getFullStackTrace, logger::info) */
    public static InputStream wrap(InputStream proxy, Function<? super Throwable, String> toString, Consumer<? super String> logger) {
        return new InputStreamWithCloseLogging(proxy, (creation, closing) -> {
            String creationStr = toString.apply(creation);
            String closingStr = toString.apply(closing);
            String msg = "Closing stream created at:\n" + creationStr + " with close called at:\n" + closingStr;

            logger.accept(msg);
        });
    }

    /*
    public static InputStream failOnClose(InputStream proxy) {
        return new InputStreamWithCloseLogging(proxy, (creation, closing) -> {
            throw new RuntimeException("close() was unexpectedly called");
        });
    }
     */

}
