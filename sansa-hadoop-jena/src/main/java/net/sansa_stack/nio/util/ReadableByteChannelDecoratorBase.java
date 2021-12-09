package net.sansa_stack.nio.util;

import java.nio.channels.ReadableByteChannel;

public class ReadableByteChannelDecoratorBase<T extends ReadableByteChannel>
    implements ReadableByteChannelDecorator
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
