package net.sansa_stack.hadoop.core.pattern;

import java.util.regex.Matcher;

public class CustomMatcherJava
    implements CustomMatcher
{
    protected Matcher matcher;

    public CustomMatcherJava(Matcher matcher) {
        this.matcher = matcher;
    }

    @Override
    public void region(int start, int end) {
        matcher.region(start, end);
    }

    @Override
    public boolean find() {
        return matcher.find();
    }

    @Override
    public int start() {
        return matcher.start();
    }

    @Override
    public int start(int group) {
        return matcher.start(group);
    }

    @Override
    public int end() {
        return matcher.end();
    }

    @Override
    public int end(int group) {
        return matcher.end(group);
    }

    @Override
    public String group() {
        return matcher.group();
    }

    @Override
    public String group(int group) {
        return matcher.group(group);
    }

    @Override
    public int groupCount() {
        return matcher.groupCount();
    }

    @Override
    public int start(String name) {
        return matcher.start(name);
    }

    @Override
    public int end(String name) {
        return matcher.end(name);
    }
}
