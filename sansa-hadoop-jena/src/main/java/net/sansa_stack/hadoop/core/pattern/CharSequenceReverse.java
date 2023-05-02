package net.sansa_stack.hadoop.core.pattern;

import com.google.common.base.Preconditions;

import java.util.stream.IntStream;

public class CharSequenceReverse
    implements CharSequence
{
    protected CharSequence base;
    // The offset from where to read in reverse
    protected int reverseStart;
    protected int reverseEnd; // 0 <= reverseEnd <= reverseStart

    public CharSequenceReverse(CharSequence base, int reverseStart) {
        this(base, reverseStart, -1);
    }

    /** Create a char sequence from the start offset <b>down to</b> the end offset */
    public CharSequenceReverse(CharSequence base, int reverseStart, int reverseEnd) {
        this.base = base;
        this.reverseStart = reverseStart; // Start is inclusive
        this.reverseEnd = reverseEnd; // End is exclusive; so by default it is at -1

        // reverseStart is inclusive
        // String rstr = new StringBuilder(((String) base).substring(reverseEnd + 1, reverseStart + 1)).reverse().toString();
        // System.out.println("Reverse string - len=" + rstr.length() + " start: " + reverseStart + " end: " + reverseEnd);
        // System.out.println(rstr);

        Preconditions.checkArgument(reverseEnd >= -1, String.format("reverseEnd (%d) must be >= -1", reverseEnd));
        Preconditions.checkArgument(reverseStart >= reverseEnd, String.format("reverseStart (%d) must be >= reverseEnd (%d)", reverseStart, reverseEnd));
    }

    @Override
    public int length() {
        return reverseStart - reverseEnd;
    }

    @Override
    public char charAt(int index) {
        return base.charAt(reverseStart - index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        Preconditions.checkArgument(start <= end, String.format("start (%d) must be <= end (%d)", start, end));
        int effStart = reverseStart - start;
        int effEnd = reverseStart - end;
        Preconditions.checkArgument(effStart >= 0 && effEnd >= -1, String.format("Effective start (%d) and effective end (%d) must be >= 0", effStart, effEnd));
        return new CharSequenceReverse(base, effStart, effEnd);
    }

    @Override
    public String toString() {
        String result = CharSequences.toString(this);
        return result;
    }

    @Override
    public IntStream chars() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public IntStream codePoints() {
        throw new UnsupportedOperationException("not implemented");
    }
}
