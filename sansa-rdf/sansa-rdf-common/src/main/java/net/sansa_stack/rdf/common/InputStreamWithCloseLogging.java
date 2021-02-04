package net.sansa_stack.rdf.common;

import org.apache.commons.io.input.ProxyInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;
import java.util.function.Function;

/** Util class to debug a stream already closed exception */
public class InputStreamWithCloseLogging
    extends ProxyInputStream
{
    protected Consumer<? super Throwable> stackTraceConsumer;

    public InputStreamWithCloseLogging(InputStream proxy, Consumer<? super Throwable> stackTraceConsumer) {
        super(proxy);
        this.stackTraceConsumer = stackTraceConsumer;
    }

    @Override
    public void close() throws IOException {
        stackTraceConsumer.accept(new Throwable());
        super.close();
    }

    /** Convenience method for e.g. .wrap(inputStream, ExceptionUtils::getFullStackTrace, logger::info) */
    public static InputStream wrap(InputStream proxy, Function<? super Throwable, String> toString, Consumer<? super String> logger) {
        return new InputStreamWithCloseLogging(proxy, t -> {
            String str = toString.apply(t);
            logger.accept(str);
        });
    }
}
