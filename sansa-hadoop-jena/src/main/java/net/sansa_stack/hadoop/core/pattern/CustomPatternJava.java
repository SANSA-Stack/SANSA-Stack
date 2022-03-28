package net.sansa_stack.hadoop.core.pattern;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomPatternJava
    implements CustomPattern
{
    protected Pattern pattern;

    public CustomPatternJava(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public CustomMatcher matcher(CharSequence charSequence) {
        Matcher m = pattern.matcher(charSequence);
        return new CustomMatcherJava(m);
    }

    /** Convenience factory methods */

    public static CustomPatternJava compile(String regex) {
        return new CustomPatternJava(Pattern.compile(regex));
    }

    public static CustomPatternJava compile(String regex, int flags) {
        return new CustomPatternJava(Pattern.compile(regex, flags));
    }
}
