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
                boolean foundCandiateRowStart = fwdMatcher.find();
                if (foundCandiateRowStart) {
                    int fwdMatchPos = fwdMatcher.start();
                    if (fwdMatchPos > regionStart) {
                        // The reverseStartPos is one char before the fwdMatchPos
                        int reverseStartPos = fwdMatchPos - 1;
                        // Some CSV test cases rely on transient bounds; i.e. the filter regex is allowed to see the bytes
                        // before the regionStart; i.e. the range [0, this.regionStart]

                        // TODO Transient/opaque bounds should be made configurable
                        boolean transientBounds = true;
                        CustomMatcher bwdMatcher;
                        if (transientBounds) {
                            CharSequence reverseCharSequence = CharSequences.reverse(charSequence, reverseStartPos);
                            bwdMatcher = bwdPattern.matcher(reverseCharSequence);
                            System.out.println("Reverse start pos: " + reverseStartPos);
                            System.out.println(reverseCharSequence.subSequence(0, Math.min(reverseCharSequence.length(), 10)).toString());
                        } else {
                            CharSequence reverseCharSequence = CharSequences.reverse(charSequence, reverseStartPos, regionStart);
                            bwdMatcher = bwdPattern.matcher(reverseCharSequence);
                            bwdMatcher.region(0, reverseStartPos - regionStart);
                        }
                        if (bwdMatcher.find()) {
                            int bwdMatchStart = bwdMatcher.start();
                            System.out.println("Found opening quote at " + bwdMatchStart);
                            continue;
                        } else {
                            result = true;
                            break;
                        }
                    } else {
                        // No data available for matching in reverse - accept the found match
                        result = true;
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
