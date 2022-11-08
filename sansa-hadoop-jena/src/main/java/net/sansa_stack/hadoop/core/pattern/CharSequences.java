package net.sansa_stack.hadoop.core.pattern;

public class CharSequences {
	public static CharSequence reverse(CharSequence base, int reverseStart) {
		return new CharSequenceReverse(base, reverseStart);
	}

	public static CharSequence reverse(CharSequence base, int reverseStart, int reverseEnd) {
		return new CharSequenceReverse(base, reverseStart, reverseEnd);
	}
}
