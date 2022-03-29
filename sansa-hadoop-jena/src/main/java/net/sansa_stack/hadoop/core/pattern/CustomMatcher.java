package net.sansa_stack.hadoop.core.pattern;

import java.util.regex.MatchResult;

public interface CustomMatcher
    extends MatchResult
{
    void region(int start, int end);
    boolean find();

}
