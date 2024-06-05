package net.sansa_stack.hadoop.core;

import org.aksw.commons.io.input.SeekableReadableChannel;
import net.sansa_stack.hadoop.core.pattern.CustomMatcher;

import java.util.function.Function;

public interface MatcherFactory
    extends Function<SeekableReadableChannel<byte[]>, CustomMatcher>
{
}
