package net.sansa_stack.hadoop.core.pattern;

public abstract class CustomMatcherBase
    implements CustomMatcher
{
    protected CharSequence charSequence;
    protected int regionStart;
    protected int regionEnd;
    protected int pos;

    public CustomMatcherBase(CharSequence charSequence) {
        this.charSequence = charSequence;
        this.regionStart = 0;
        this.regionEnd = charSequence.length();
    }

    @Override
    public void region(int start, int end) {
        this.regionStart = start;
        this.regionEnd = end;

        this.pos = start;
    }

    @Override
    public int start(int group) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int end(int group) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String group(int group) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int groupCount() {
        return 0;
    }

    @Override
    public int start(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int end(String name) {
        throw new UnsupportedOperationException();
    }
}
