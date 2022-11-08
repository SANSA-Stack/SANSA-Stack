package net.sansa_stack.hadoop.core.pattern;

import java.util.Objects;

public class CustomPatternFiltered
	implements CustomPattern
{
	protected CustomPattern fwdPattern;
	
	// Exception pattern such as whether the fwdPattern is escaped. A match of this pattern invalidates the one by the fwdPattern.
	protected CustomPattern bwdPattern;

	public CustomPatternFiltered(CustomPattern fwdPattern, CustomPattern bwdPattern) {
		super();
		Objects.requireNonNull(fwdPattern);
		Objects.requireNonNull(bwdPattern);
		this.fwdPattern = fwdPattern;
		this.bwdPattern = bwdPattern;
	}

	@Override
	public CustomMatcher matcher(CharSequence charSequence) {
		return new CustomMatcherFilter(charSequence);
	}
	
	public class CustomMatcherFilter
		extends CustomMatcherBase
	{
		protected CustomMatcher fwdMatcher;
		
		public CustomMatcherFilter(CharSequence charSequence) {
			super(charSequence);
			this.fwdMatcher = fwdPattern.matcher(charSequence);
		}
		
		@Override
		public void region(int start, int end) {
			fwdMatcher.region(start, end);
			super.region(start, end);
		}

		@Override
		public boolean find() {
			boolean result = false;
			while (true) {
				// Result is set to true tentatively; will be reset to false if the bwd pattern matches
				result = fwdMatcher.find();
				if (result) {
					int p = fwdMatcher.start();
					if (p > regionStart) {
						int reverseStartPos = p - 1;
						// Some CSV test cases rely on transient bounds; i.e. the filter regex is allowed to see the bytes
						// before the regionStart; i.e. the range [0, this.regionStart]
						boolean transientBounds = true;
						CustomMatcher bwdMatcher;
						if (transientBounds) {
							CharSequence reverseCharSequence = CharSequences.reverse(charSequence, reverseStartPos);
							bwdMatcher = bwdPattern.matcher(reverseCharSequence);
						} else {
							// This could be made configurable
							CharSequence reverseCharSequence = CharSequences.reverse(charSequence, reverseStartPos, regionStart);
							bwdMatcher = bwdPattern.matcher(reverseCharSequence);
							bwdMatcher.region(0, reverseStartPos - regionStart);
						}
						if (bwdMatcher.find()) {
							result = false;
							continue;
						} else {
							break;
						}
					} else {
						// No data available for matching in reverse - accept the found match
						break;
					}
				} else {
					// No match at all
					break;
				}
			}
			return result;
		}

		@Override
		public int start() {
			int result = fwdMatcher.start();
			return result;
		}

		@Override
		public int end() {
			int result = fwdMatcher.end();
			return result;
		}

		@Override
		public String group() {
			String result = fwdMatcher.group();
			return result;
		}
	}
}
