package net.sansa_stack.rdf.common;

import java.nio.channels.ReadableByteChannel;

public class ReadableByteChannelDecoratorBase<T extends ReadableByteChannel>
    implements ReadableByteChannelDecorator<T>
{
    protected T delegate;

    public ReadableByteChannelDecoratorBase(T delegate) {
        this.delegate = delegate;
    }

    @Override
    public T getDelegate() {
        return delegate;
    }
}
