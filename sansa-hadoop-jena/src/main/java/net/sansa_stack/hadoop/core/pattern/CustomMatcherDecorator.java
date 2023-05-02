package net.sansa_stack.hadoop.core.pattern;

public abstract class CustomMatcherDecorator
    implements CustomMatcher
{
    protected abstract CustomMatcher getDecoratee();

    @Override
    public void region(int start, int end) {
        getDecoratee().region(start, end);
    }

    @Override
    public boolean find() {
        return getDecoratee().find();
    }

    @Override
    public int start() {
        return getDecoratee().start();
    }

    @Override
    public int start(int group) {
        return getDecoratee().start(group);
    }

    @Override
    public int end() {
        return getDecoratee().end();
    }

    @Override
    public int end(int group) {
        return getDecoratee().end(group);
    }

    @Override
    public String group() {
        return getDecoratee().group();
    }

    @Override
    public String group(int group) {
        return getDecoratee().group(group);
    }

    @Override
    public int groupCount() {
        return getDecoratee().groupCount();
    }
}
