package net.sansa_stack.hadoop.core.pattern;

public abstract class CustomPatternDecoratorBase
    implements CustomPattern
{
    protected CustomPattern pattern;

    public CustomPatternDecoratorBase(CustomPattern pattern) {
        super();
        this.pattern = pattern;
    }
}
