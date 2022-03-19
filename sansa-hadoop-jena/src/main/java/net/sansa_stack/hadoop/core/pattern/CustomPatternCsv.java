package net.sansa_stack.hadoop.core.pattern;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Matcher that searches for record starts (the first character following a newline) that are NOT within a
 * multiline field.
 */
public class CustomPatternCsv
    implements CustomPattern
{
    public static class Config {
        protected char quoteChar;
        protected char escapeChar;
        protected String delimiter;
        protected String lineTerminatorPattern;
        protected int maxConsecutiveEscapeChars;

        public Config(char quoteChar, char escapeChar, String delimiter, String lineTerminatorPattern, int maxConsecutiveEscapeChars) {
            this.quoteChar = quoteChar;
            this.escapeChar = escapeChar;
            this.delimiter = delimiter;
            this.lineTerminatorPattern = lineTerminatorPattern;
            this.maxConsecutiveEscapeChars = maxConsecutiveEscapeChars;
        }

        public static Config createExcel() {
            // return new Config('"', '"', ",", "\r?\n\r?", 32);
            return create('"');
        }

        public static Config create(char quoteChar) {
            return create(quoteChar, quoteChar);
        }

        public static Config create(char quoteChar, char escapeChar) {
            return new Config(quoteChar, escapeChar, ",", "\r?\n\r?", 32);
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

        public int getMaxConsecutiveEscapeChars() {
            return maxConsecutiveEscapeChars;
        }
    }


    // TODO finish the pattern
    // In reverse matching the escape character is seen after the quote character
    public static Pattern createBwdQuotePattern(Config dialect) {
        String patternStr = "(quote)((term)|(delim)|$)";

        String str = patternStr
                .replace("(quote)", Pattern.quote(Character.toString(dialect.getQuoteChar())))
                .replace("(delim)", Pattern.quote(dialect.getDelimiter()))
                .replace("(term)", dialect.getLineTerminatorPattern());

        Pattern result = Pattern.compile(str, Pattern.DOTALL | Pattern.MULTILINE);
        return result;
    }

    public static Pattern createFwdQuotePattern(Config dialect) {
        // String equoteFieldEnd = "(?<!((?<!\\\\)(\\\\\\\\){0,10}))\"(\r?\n|,|$)";
        String patternStr = "(?<!((?<!(esc))((esc)(esc)){0,(nesc)})(esc))(quote)((term)|(delim)|$)";

        String str = patternStr;
        str = str.replace("(esc)", Pattern.quote(Character.toString(dialect.getEscapeChar())));
        str = str.replace("(quote)", Pattern.quote(Character.toString(dialect.getQuoteChar())));
        str = str.replace("(delim)", dialect.getDelimiter());
        str = str.replace("(term)", dialect.getLineTerminatorPattern());
        str = str.replace("(nesc)", Integer.toString(dialect.getMaxConsecutiveEscapeChars()));

        // System.out.println(str);
        Pattern result = Pattern.compile(str, Pattern.DOTALL | Pattern.MULTILINE);
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


    public static CustomPatternCsv create(Config dialect) {
        Pattern firstCharOnNewLinePattern;
        Pattern endOfQuotedFieldFwdPattern;
        Pattern startOfQuotedFieldBwdPattern;

        String str = "(?<=" + dialect.getLineTerminatorPattern() + ").";
        // System.out.println(Pattern.quote(str));
        firstCharOnNewLinePattern = Pattern.compile(str, Pattern.DOTALL);

        char quoteChar = dialect.getQuoteChar();
        char escapeChar = dialect.getEscapeChar();
        if (quoteChar == escapeChar) {
            endOfQuotedFieldFwdPattern = createFwdQuotePattern(dialect);
            startOfQuotedFieldBwdPattern = endOfQuotedFieldFwdPattern;
        } else {
            endOfQuotedFieldFwdPattern = createFwdQuotePattern(dialect);
            startOfQuotedFieldBwdPattern = createBwdQuotePattern(dialect);
        }

        return new CustomPatternCsv(firstCharOnNewLinePattern, endOfQuotedFieldFwdPattern, startOfQuotedFieldBwdPattern);

    }

    protected Pattern firstCharOnNewLinePattern;
    protected Pattern endOfQuotedFieldFwdPattern;
    protected Pattern startOfQuotedFieldBwdPattern;

    public CustomPatternCsv(Pattern firstCharAfterNewlinePattern, Pattern endOfQuotedFieldFwdPattern, Pattern startOfQuotedFieldBwdPattern) {
        this.firstCharOnNewLinePattern = firstCharAfterNewlinePattern;
        this.endOfQuotedFieldFwdPattern = endOfQuotedFieldFwdPattern;
        this.startOfQuotedFieldBwdPattern = startOfQuotedFieldBwdPattern;
    }

    @Override
    public CustomMatcher matcher(CharSequence charSequence) {
        return new CustomMatcherCsv(charSequence);
    }

    public class CustomMatcherCsv
        implements CustomMatcher
    {
        protected CharSequence charSequence;
        protected int regionStart;
        protected int regionEnd;
        protected int newlineMatchStart = -1;

        protected boolean nextQuoteExamined = false;
        protected int nextQuoteEnd = -1;
        protected int nextQuoteStart = -1;

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
            // String equoteFieldEnd = "(?<!((?<!\")(\"\"){0,10})\")\"(\r?\n|,|$)";

            // Pattern equoteFieldEndPattern = Pattern.compile(equoteFieldEnd, Pattern.DOTALL | Pattern.MULTILINE);

            // Pattern newline = Pattern.compile("\n.", Pattern.DOTALL);
            Matcher firstCharOnNewLineMatcher =  firstCharOnNewLinePattern.matcher(charSequence);
            firstCharOnNewLineMatcher.region(pos, regionEnd);

            int firstCharPos = -1;
            while (true) {
                if (firstCharOnNewLineMatcher.find()) {
                    firstCharPos = firstCharOnNewLineMatcher.start();

                    if (firstCharPos < nextQuoteStart) {
                        // the newline should be a safe match
                        break;
                    } else if (firstCharPos < nextQuoteEnd) {
                        // search the next line after the quot end
                        nextQuoteExamined = false;
                        pos = nextQuoteEnd + 1;
                        firstCharOnNewLineMatcher.region(pos, regionEnd);
                        continue;
                    }

                    if (!nextQuoteExamined) {
                        // Check if there is an equote before a field delimiter
                        Matcher endMatcher = endOfQuotedFieldFwdPattern.matcher(charSequence);
                        endMatcher.region(firstCharPos, regionEnd);
                        // endMatcher.region(firstCharPos + 1, regionEnd);
                        if (endMatcher.find()) {
                            nextQuoteEnd = endMatcher.start();

                            int reverseRegionStart = nextQuoteEnd - 1;
                            int reverseSearchLength = reverseRegionStart - firstCharPos;

                            // Is there a matching equote?
                            CharSequence reverse = new CharSequenceReverse(charSequence, reverseRegionStart);
                            Matcher startMatcher = startOfQuotedFieldBwdPattern.matcher(reverse);
                            startMatcher.region(0, reverseSearchLength);
                            // int startPos = -1;
                            if (startMatcher.find()) {
                                int reverseMatchStart = startMatcher.start();
                                nextQuoteStart = nextQuoteEnd - reverseMatchStart;
                            }


                            // If the startPos is after the newline then we are not in a cell
                            // Otherwise continue searching for newlines after the endPos
                            if (nextQuoteStart > firstCharPos) {
                                pos = firstCharPos + 1;
                                break;
                            } else {
                                pos = nextQuoteEnd + 1;
                                nextQuoteExamined = false;
                                firstCharOnNewLineMatcher.region(pos, regionEnd);
                            }

                        } else {
                            // If there is no known end then accept the newline pos
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    firstCharPos = -1;
                    break;
                }
            }

            boolean result = firstCharPos != -1;
            newlineMatchStart = firstCharPos;

            if (!result) {
                pos = -1;
            } else {
                pos = firstCharPos + 1;
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
