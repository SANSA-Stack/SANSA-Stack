package net.sansa_stack.hadoop.core.pattern;

public class CharSequences {
    public static CharSequence reverse(CharSequence base, int reverseStart) {
        return new CharSequenceReverse(base, reverseStart);
    }

    public static CharSequence reverse(CharSequence base, int reverseStart, int reverseEnd) {
        return new CharSequenceReverse(base, reverseStart, reverseEnd);
    }

    public static String toString(CharSequence base) {
        int l = base.length();
        char[] chars = new char[l];
        for (int i = 0 ; i < l; ++i) {
            chars[i] = base.charAt(i);
        }
        String result = new String(chars, 0, l);
        return result;
    }

    public static boolean isValidPos(CharSequence cs, int offset) {
        return offset >= 0 && offset < cs.length();
    }

    /** Returns -1 if offset out of bounds */
    public static int charAt(CharSequence cs, int offset) {
        int result = isValidPos(cs, offset) ? cs.charAt(offset) : -1;
        return result;
    }
}
