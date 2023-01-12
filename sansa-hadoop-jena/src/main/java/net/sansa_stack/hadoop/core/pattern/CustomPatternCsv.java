package net.sansa_stack.hadoop.core.pattern;

import java.util.regex.Pattern;

/**
 * Matcher that searches for record starts (the first character following a newline) that are NOT within a
 * multiline field.
 *
 * FIXME: Should this pattern match "end of file"? A problem arises if no newline exists: does that mean that there too much data without newline or did we hit the end of file?
 *
 */
public class CustomPatternCsv
    implements CustomPattern
{
    public static class Config {
        protected char quoteChar;
        protected char escapeChar;
        protected String delimiter; // field separator
        protected String lineTerminatorPattern;
        protected int maxConsecutiveEscapeChars;
        protected int columnMaxLength;

        public Config(char quoteChar, char escapeChar, String delimiter, String lineTerminatorPattern, int columnMaxLength, int maxConsecutiveEscapeChars) {
            this.quoteChar = quoteChar;
            this.escapeChar = escapeChar;
            this.delimiter = delimiter;
            this.lineTerminatorPattern = lineTerminatorPattern;
            this.columnMaxLength = columnMaxLength;
            this.maxConsecutiveEscapeChars = maxConsecutiveEscapeChars;
        }

        public static Config createExcel(int columnMaxLength) {
            return create('"', columnMaxLength);
        }

        public static Config create(char quoteChar, int columnMaxLength) {
            return create(quoteChar, quoteChar, columnMaxLength);
        }

        public static Config create(char quoteChar, char escapeChar, int columnMaxLength) {
            return new Config(quoteChar, escapeChar, Pattern.quote( ","), "\r?\n\r?", columnMaxLength, 32);
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

        public int getColumnMaxLength() {
            return columnMaxLength;
        }
    }

    /** Pattern for matching an effective quote in forward direction.
     * An effective quote is a quote that is not escaped. */
    public static CustomPattern createFwdQuotePattern(Config dialect) {
        // The part after the quote is in a lookahead in order to match that part with transient bounds
        String patternStr = "(quote)(?=((term)|(delim)|$))";
        String str = substitute(patternStr, dialect);
        return CustomPatternJava.compile(str, Pattern.DOTALL | Pattern.MULTILINE);
    }

    // In reverse matching the escape character is seen after the quote character
    public static CustomPattern createBwdPatternClosingQuote(Config dialect) {
        String patternStr = "(quote)(?=((term)|(delim)|$))";
        String str = substitute(patternStr, dialect);
        CustomPattern result = CustomPatternJava.compile(str, Pattern.DOTALL | Pattern.MULTILINE);
        return result;
    }

    // Closing quote:
    // Match a separator or newline followed by an effective quote TODO Is that correct?
    // ((newline)|(sep))(quote)(?!((esc)(esc))+)
    public static CustomPattern createBwdPatternClosingQuote2(Config dialect) {
        String patternStr = "(quote)(?=((term)|(delim)|$))";
        String str = substitute(patternStr, dialect);
        CustomPattern result = CustomPatternJava.compile(str, Pattern.DOTALL | Pattern.MULTILINE);
        return result;
    }

    public static CustomPattern createIsEscapedBwdPattern(Config dialect) {
        // A quote is NOT escaped if it is preceded by an odd number of escape symbols
        String isEscapedBwdPatternStr = "^((esc)(esc))*(esc)([^(esc)]|$)";
        String str = substitute(isEscapedBwdPatternStr, dialect);
        return CustomPatternJava.compile(str, Pattern.DOTALL | Pattern.MULTILINE);
    }

    public static String substitute(String patternStr, Config dialect) {
        String str = patternStr;
        str = str.replace("(esc)", Pattern.quote(Character.toString(dialect.getEscapeChar()))); // escape character as pattern literal
        str = str.replace("(quote)", Pattern.quote(Character.toString(dialect.getQuoteChar())));
        str = str.replace("(delim)", dialect.getDelimiter());
        str = str.replace("(term)", dialect.getLineTerminatorPattern());
        str = str.replace("(nesc)", Integer.toString(dialect.getMaxConsecutiveEscapeChars()));
        return str;
    }
//  String str = patternStr;
//  str = str.replace("(quote)", Pattern.quote(Character.toString(dialect.getQuoteChar())));
//  str = str.replace("(delim)", Pattern.quote(dialect.getDelimiter()));
//  str = str.replace("(term)", dialect.getLineTerminatorPattern());

    public static CustomPattern createFwdQuotePatternOld(Config dialect) {
        // String equoteFieldEnd = "(?<!((?<!\\\\)(\\\\\\\\){0,10}))\"(\r?\n|,|$)";
        String patternStr = "(?<!((?<!(esc))((esc)(esc)){0,(nesc)})(esc))(quote)((term)|(delim)|$)";
        //String patternStr = "(?<!((?<!(esc))((esc)(esc)){0,(nesc)})(esc))((quote)((term)|(delim)|$))";
        //String patternStr = "(quote)((term)|(delim)|$)";
        String str = substitute(patternStr, dialect);
        return CustomPatternJava.compile(str, Pattern.DOTALL | Pattern.MULTILINE);
    }

    public static CustomPatternCsv create(Config dialect) {
        CustomPattern firstCharOnNewLinePattern;
        CustomPattern endOfQuotedFieldFwdPattern;
        CustomPattern startOfQuotedFieldBwdPattern;

        String str = "(?<=" + dialect.getLineTerminatorPattern() + ").";
        // System.out.println(Pattern.quote(str));
        firstCharOnNewLinePattern = CustomPatternJava.compile(str, Pattern.DOTALL);

        char quoteChar = dialect.getQuoteChar();
        char escapeChar = dialect.getEscapeChar();
        if (quoteChar == escapeChar) {
            endOfQuotedFieldFwdPattern = new CustomPatternFiltered(
                    createFwdQuotePattern(dialect),
                    createIsEscapedBwdPattern(dialect)
            );
            // endOfQuotedFieldFwdPattern = createFwdQuotePattern(dialect);
            startOfQuotedFieldBwdPattern = endOfQuotedFieldFwdPattern;
        } else {
            // Slight limitation: The assumption here is that the field separator is not the escape char
            // So if we find {@code ,"} then the double quote is a record offset
            // endOfQuotedFieldFwdPattern = createFwdQuotePattern(dialect);
            endOfQuotedFieldFwdPattern = new CustomPatternFiltered(
                    createFwdQuotePattern(dialect),
                    createIsEscapedBwdPattern(dialect)
            );

            // endOfQuotedFieldFwdPattern = createFwdQuotePattern(dialect);
            startOfQuotedFieldBwdPattern = createBwdPatternClosingQuote(dialect);
        }

        return new CustomPatternCsv(dialect.getColumnMaxLength(), firstCharOnNewLinePattern, endOfQuotedFieldFwdPattern, startOfQuotedFieldBwdPattern);
    }

    protected int columnMaxLength;
    protected CustomPattern firstCharOnNewLinePattern;
    protected CustomPattern endOfQuotedFieldFwdPattern;
    protected CustomPattern startOfQuotedFieldBwdPattern;

    public CustomPatternCsv(int columnMaxLength, CustomPattern firstCharAfterNewlinePattern, CustomPattern endOfQuotedFieldFwdPattern, CustomPattern startOfQuotedFieldBwdPattern) {
        this.firstCharOnNewLinePattern = firstCharAfterNewlinePattern;
        this.endOfQuotedFieldFwdPattern = endOfQuotedFieldFwdPattern;
        this.startOfQuotedFieldBwdPattern = startOfQuotedFieldBwdPattern;
        this.columnMaxLength = columnMaxLength;
    }

    @Override
    public CustomMatcher matcher(CharSequence charSequence) {
        return new CustomMatcherCsv(charSequence);
        // return new CustomMatcherCsv2(charSequence, null);
    }

    public class CustomMatcherCsv
        extends CustomMatcherBase
    {
        protected int newlineMatchStart = -1;
        protected boolean nextQuoteExamined = false;
        protected int nextQuoteEnd = -1;
        protected int nextQuoteStart = -1;

        public CustomMatcherCsv(CharSequence charSequence) {
            super(charSequence);
        }


        /**
         * The find method operates as follows:
         *
         * 1.) set startPos to 0
         * 2.) Find the first character after a newline after startPos - save this position and candidatePos
         * 3.) Verify that the newline is a new row start:
         *       Check backwards up to the startPos (not exceeding it) - if there is no start of an escaped field then the newline is a row start and break
         * 4.) Set startPos to candidatePos and got to 2
         *
         * Notes:
         * - An empty quoted field: Assume the following data:
         * "","",hello
         * "",""$
         * The issue is that a quoted empty field may look exactly like an escaped double quote.
         *
         */
        @Override
        public boolean find() {
            if (pos < 0) {
                return false;
            }

            CustomMatcher firstCharOnNewLineMatcher = firstCharOnNewLinePattern.matcher(charSequence);
            firstCharOnNewLineMatcher.region(pos, regionEnd);

            int firstCharPos = -1;
            while (true) {
                if (firstCharOnNewLineMatcher.find()) {
                    firstCharPos = firstCharOnNewLineMatcher.start();

                    if (firstCharPos < nextQuoteStart) {
                        // the newline should be a safe match
                        break;
                    } else if (firstCharPos < nextQuoteEnd) {
                        // search the next line after the quote end
                        nextQuoteExamined = false;
                        pos = nextQuoteEnd + 1;

                        int requestedLength = regionEnd - pos;

                        int relPos = pos - regionStart;
                        int allowedLength = Math.min(requestedLength, columnMaxLength - relPos);

                        // firstCharOnNewLineMatcher.region(pos, regionEnd);
                        firstCharOnNewLineMatcher.region(pos, pos + allowedLength);
                        continue;
                    }

                    if (!nextQuoteExamined) {
                        // Check if there is an effective end quote before a field delimiter
                        CustomMatcher endMatcher = endOfQuotedFieldFwdPattern.matcher(charSequence);
                        endMatcher.region(firstCharPos, regionEnd);
                        // endMatcher.region(firstCharPos + 1, regionEnd);
                        if (endMatcher.find()) {
                            nextQuoteEnd = endMatcher.start();

                            int reverseRegionStart = nextQuoteEnd - 1;
                            int reverseSearchLength = reverseRegionStart - firstCharPos;

                            // Is there a matching effective starting quote?
                            // TODO We should check for such candidate offsets during the forward scan
                            CharSequence reverse = new CharSequenceReverse(charSequence, reverseRegionStart);
                            CustomMatcher startMatcher = startOfQuotedFieldBwdPattern.matcher(reverse);
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
        public int end() {
            return newlineMatchStart + 1;
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
    }

    // Create a pattern for matching csv-like double quotes - where
    // double-double quote escapes another double quote
    // This pattern works for forward and reverse matching
    /*
    public static Pattern createQuotePattern(char quoteChar) {
        String equoteFieldEnd = "(?<!((?<!\")(\"\"){0,10})\")\"(\r?\n|,|$)";

        if (quoteChar != '"') {
            String qcp = Pattern.quote(Character.toString(quoteChar));
            equoteFieldEnd = equoteFieldEnd.replace("\"", qcp);
        }

        Pattern result = Pattern.compile(equoteFieldEnd);
        return result;
    }
    */
    // createFwdQuotePattern('\"', '\\');
    // Match the first equote of a sequence of possibly escaped quote chars
    // String equoteStart = "((?<!\")\"(?=(\"\"){0,10}(\r?\n|,|$)))";
    // String matchCharAfterNewline = "${equoteFirst}(\r?\n|,|$))).";
    // Pattern equoteStartPattern = Pattern.compile(equoteStart, Pattern.DOTALL | Pattern.MULTILINE);

    // Match an equote at the end of an field
    // String equoteFieldEnd = "(?<!((?<!\")(\"\"){0,10})\")\"(\r?\n|,|$)";

    // Pattern equoteFieldEndPattern = Pattern.compile(equoteFieldEnd, Pattern.DOTALL | Pattern.MULTILINE);

    // Pattern newline = Pattern.compile("\n.", Pattern.DOTALL);
}
