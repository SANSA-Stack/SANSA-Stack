package net.sansa_stack.nio.util;

import net.sansa_stack.nio.util.SeekableByteChannelDecorator;

import java.nio.channels.SeekableByteChannel;

public class SeekableByteChannelDecoratorBase<T extends SeekableByteChannel>
    implements SeekableByteChannelDecorator
{
    protected T decoratee;

    public SeekableByteChannelDecoratorBase(T decoratee) {
        super();
        this.decoratee = decoratee;
    }

    @Override
    public T getDecoratee() {
        return decoratee;
    }
}
