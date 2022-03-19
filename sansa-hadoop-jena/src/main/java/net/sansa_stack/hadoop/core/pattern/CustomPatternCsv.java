package net.sansa_stack.hadoop.core.pattern;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Matcher that searches for newlines that are NOT within a multiline field.
 *
 *
 */
public class CustomPatternCsv
    implements CustomPattern
{
    public static class Config {
        protected char quoteChar;
        protected char escapeChar;
        protected String delimiter;
        protected String lineTerminatorPattern;

        public Config(char quoteChar, char escapeChar, String delimiter, String lineTerminatorPattern) {
            this.quoteChar = quoteChar;
            this.escapeChar = escapeChar;
            this.delimiter = delimiter;
            this.lineTerminatorPattern = lineTerminatorPattern;
        }

        public static Config createDefault() {
            return new Config('"', '\\', ",", "\r?\n\r?");
        }

        public char getQuoteChar() {
            return quoteChar;
        }

        public char getEscapeChar() {
            return escapeChar;
        }

        public String getDelimiter() {
            return delimiter;
        }

        public String getLineTerminatorPattern() {
            return lineTerminatorPattern;
        }
    }


    public static CustomPatternCsv create(Config dialect) {
        return null;
    }


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
        protected int newlineMatchStart = -1;
        // protected int matchEnd = -1;


        protected boolean nextQuoteExamined = false;
        protected int nextQuoteEnd = -1;
        protected int nextQuoteStart = -1;

        protected char quoteChar;
        protected char quoteEscapeChar;

        protected int pos;


        public CustomMatcherCsv(CharSequence charSequence) {
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


        // TODO finish the pattern
        // In reverse matching the escape character is seen after the quote character
        public static Pattern createBwdQuotePattern(char quoteChar, char escapeChar) {
            String equoteFieldEnd = "\"(\r?\n|,|$)";
            return null;
        }

        public static Pattern createFwdQuotePattern(char quoteChar, char escapeChar) {
            String equoteFieldEnd = "(?<!((?<!\\\\)(\\\\\\\\){0,10}))\"(\r?\n|,|$)";

            if (quoteChar != '"') {
                String qcp = Pattern.quote(Character.toString(quoteChar));
                equoteFieldEnd = equoteFieldEnd.replace("\"", qcp);
            }

            if (escapeChar != '\\') {
                String ecp = Pattern.quote(Character.toString(escapeChar));
                equoteFieldEnd = equoteFieldEnd.replace("\\\\", ecp);
            }

            Pattern result = Pattern.compile(equoteFieldEnd);
            return result;
        }

        // Create a pattern for matching csv-like double quotes - where
        // double-double quote escapes another double quote
        // This pattern works for forward and reverse matching
        public static Pattern createQuotePattern(char quoteChar) {
            String equoteFieldEnd = "(?<!((?<!\")(\"\"){0,10})\")\"(\r?\n|,|$)";

            if (quoteChar != '"') {
                String qcp = Pattern.quote(Character.toString(quoteChar));
                equoteFieldEnd = equoteFieldEnd.replace("\"", qcp);
            }

            Pattern result = Pattern.compile(equoteFieldEnd);
            return result;
        }

        @Override
        public boolean find() {
            if (pos < 0) {
                return false;
            }

            // createFwdQuotePattern('\"', '\\');
            // Match the first equote of a sequence of possibly escaped quote chars
            // String equoteStart = "((?<!\")\"(?=(\"\"){0,10}(\r?\n|,|$)))";
            // String matchCharAfterNewline = "${equoteFirst}(\r?\n|,|$))).";
            // Pattern equoteStartPattern = Pattern.compile(equoteStart, Pattern.DOTALL | Pattern.MULTILINE);

            // Match an equote at the end of an field
            String equoteFieldEnd = "(?<!((?<!\")(\"\"){0,10})\")\"(\r?\n|,|$)";

            Pattern equoteFieldEndPattern = Pattern.compile(equoteFieldEnd, Pattern.DOTALL | Pattern.MULTILINE);

            Pattern newline = Pattern.compile("\n.", Pattern.DOTALL);
            Matcher newlineMatcher =  newline.matcher(charSequence);
            newlineMatcher.region(pos, regionEnd);

            int newlinePos = -1;
            while (true) {
                if (newlineMatcher.find()) {
                    newlinePos = newlineMatcher.start();

                    if (newlinePos < nextQuoteStart) {
                        // the newline should be a safe match
                        break;
                    } else if (newlinePos < nextQuoteEnd) {
                        // search the next line after the quot end
                        nextQuoteExamined = false;
                        pos = nextQuoteEnd + 1;
                        newlineMatcher.region(pos, regionEnd);
                        continue;
                    }

                    if (!nextQuoteExamined) {
                        // Check if there is an equote before a field delimiter
                        Matcher endMatcher = equoteFieldEndPattern.matcher(charSequence);
                        endMatcher.region(newlinePos + 1, regionEnd);
                        if (endMatcher.find()) {
                            nextQuoteEnd = endMatcher.start();

                            int reverseRegionStart = nextQuoteEnd - 1;
                            int reverseSearchLength = reverseRegionStart - newlinePos;

                            // Is there a matching equote?
                            CharSequence reverse = new CharSequenceReverse(charSequence, reverseRegionStart);
                            Matcher startMatcher = equoteFieldEndPattern.matcher(reverse);
                            startMatcher.region(0, reverseSearchLength);
                            // int startPos = -1;
                            if (startMatcher.find()) {
                                int reverseMatchStart = startMatcher.start();
                                nextQuoteStart = nextQuoteEnd - reverseMatchStart;
                            }


                            // If the startPos is after the newline then we are not in a cell
                            // Otherwise continue searching for newlines after the endPos
                            if (nextQuoteStart > newlinePos) {
                                pos = newlinePos + 1;
                                break;
                            } else {
                                pos = nextQuoteEnd + 1;
                                nextQuoteExamined = false;
                                newlineMatcher.region(pos, regionEnd);
                            }

                        } else {
                            // If there is no known end then accept the newline pos
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    newlinePos = -1;
                    break;
                }
            }

            boolean result = newlinePos != -1;
            newlineMatchStart = newlinePos;

            if (!result) {
                pos = -1;
            } else {
                pos = newlinePos + 1;
            }

            return result;
        }

        @Override
        public int start() {
            return newlineMatchStart;
        }

        @Override
        public int start(int group) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int end() {
            return newlineMatchStart + 1;
        }

        @Override
        public int end(int group) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String group() {
            String result;

            if (newlineMatchStart < 0) {
                throw new IllegalStateException("No match found");
            }

            result = Character.toString(charSequence.charAt(newlineMatchStart));
            return result;
            // throw new UnsupportedOperationException();
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
