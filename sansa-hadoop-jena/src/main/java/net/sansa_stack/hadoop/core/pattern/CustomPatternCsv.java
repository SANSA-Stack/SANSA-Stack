package net.sansa_stack.hadoop.core.pattern;

public class CustomPatternCsv
    implements CustomPattern
{
    @Override
    public CustomMatcher matcher(CharSequence charSequence) {
        return new CustomMatcherCsv(charSequence);
    }

    public static class CustomMatcherCsv
        implements CustomMatcher
    {
        protected CharSequence charSequence;
        protected int regionStart;
        protected int regionEnd;

        protected int matchStart;
        protected int matchEnd;

        protected char quoteChar;
        protected char quoteEscapeChar;

        public CustomMatcherCsv(CharSequence charSequence) {
            this.charSequence = charSequence;
        }

        @Override
        public void region(int start, int end) {
            this.regionStart = start;
            this.regionEnd = end;
        }

        @Override
        public boolean find() {
            // well, logic would go here

            return false;
        }

        @Override
        public int start() {
            return matchStart;
        }

        @Override
        public int start(int group) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int end() {
            return matchEnd;
        }

        @Override
        public int end(int group) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String group() {
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
    }
}
