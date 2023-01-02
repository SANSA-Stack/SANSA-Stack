package net.sansa_stack.hadoop.core.pattern;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.aksw.commons.model.csvw.domain.api.Dialect;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.CsvwLib;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableImpl;

public class CustomPatternCsv2
    implements CustomPattern
{
    protected Dialect dialect;
    protected Pattern fieldSeparatorAndNewlinePattern;

    public static Pattern createPattern(Dialect dialect) {
        List<String> ts = dialect.getLineTerminatorList();
        if (ts == null || ts.isEmpty()) {
            ts = null; // \r?\n\r?
        }

        String fieldSeparator = Optional.ofNullable(dialect.getDelimiter()).orElse(",");

        String patternStr =
            "(?<" + CsvwLib.LINE_TERMINATOR_KEY + ">" + ts.stream().map(x -> "(" + Pattern.quote(x) + ")").collect(Collectors.joining("|")) + ")|" +
            "(?<" + CsvwLib.FIELD_SEPARATOR_KEY + ">" + Pattern.quote(fieldSeparator) + ")";

        // System.out.println(patternStr);
        return Pattern.compile(patternStr, Pattern.MULTILINE | Pattern.DOTALL);
    }

    public static CustomPattern create() {
        return create(null);
    }

    public static CustomPattern create(Dialect dialect) {
        DialectMutable copy = new DialectMutableImpl();
        dialect.copyInto(copy, true);
        CsvwLib.buildEffectiveModel(dialect, copy);

        return new CustomPatternCsv2(dialect);
    }

    protected CustomPatternCsv2(Dialect dialect) {
        super();
        this.dialect = dialect;
        this.fieldSeparatorAndNewlinePattern = createPattern(dialect);
    }

    @Override
    public CustomMatcher matcher(CharSequence charSequence) {
        return new CustomMatcherCsv2(charSequence);
    }

    public class CustomMatcherCsv2
        extends CustomMatcherBase
    {
        /** The number of expected columns.
         * This value can be used to filter out ambiguous interpretations of quoted fields
         * that result in more or fewer columns */
        protected int expectedColumnCount;
        // protected Pattern pattern;
        // protected Dialect dialect;

        /** Matcher to move to the next field separator or newline */
        protected Matcher fieldSeparatorAndNewlineMatcher;

        public CustomMatcherCsv2(CharSequence charSequence) {
            super(charSequence);
            fieldSeparatorAndNewlineMatcher = fieldSeparatorAndNewlinePattern.matcher(charSequence);
        }

        /** Start of field content (excluding a possible leading a quote) */
        protected int currentFieldContentStart;
        protected boolean isInQuotedField = false;

        // protected long matchEnd = -1;
        protected int matchStart = -1;
        protected int matchEnd = -1;

        @Override
        public boolean find() {
            boolean result = false;
            char quoteChar = CsvwLib.expectAtMostOneChar("quote char", dialect.getQuoteChar());
            char escapeChar = CsvwLib.expectAtMostOneChar("escape char", dialect.getQuoteEscapeChar());

            if (pos < 0) {
                return false;
            }

            while (fieldSeparatorAndNewlineMatcher.find()) {
                matchStart = fieldSeparatorAndNewlineMatcher.start();
                matchEnd = fieldSeparatorAndNewlineMatcher.end();

                if (fieldSeparatorAndNewlineMatcher.start(CsvwLib.LINE_TERMINATOR_KEY) >= 0) {
                    // If we are in a multiline field which is NOT terminated before the newline then continue
                    // otherwise return the position after the line separator as the next row
                    if (isInQuotedField) {
                        if (isPrecededByEffectiveQuote(charSequence, matchStart, currentFieldContentStart, quoteChar, escapeChar)) {
                            // End of field
                            isInQuotedField = false;
                            result = true;
                        } else {
                            // There is a quote before a delimiter such as [",] without a multiline field
                            // it may be a trailing quote in a non-quoted field but most likely either
                            // its an error - either a data issue or our assumption being n a multline-field was wrong
                            // ++errorCount
                            continue;
                        }
                    } else {
                        result = true;
                    }
                } else if (fieldSeparatorAndNewlineMatcher.start(CsvwLib.FIELD_SEPARATOR_KEY) >= 0) {
                    if (isPrecededByEffectiveQuote(charSequence, matchStart, currentFieldContentStart, quoteChar, escapeChar)) {
                        if (!isInQuotedField) {
                            // errorCount++
                        }
                        isInQuotedField = false;
                    }
                } else {
                    throw new IllegalStateException("should not happen");
                }

                if (!isInQuotedField) {
                    // Check if the next field starts with an effective quote
                    int c = CharSequences.charAt(charSequence, matchEnd);
                    if (c == quoteChar) {
                        isInQuotedField = true;
                    } else {
                        isInQuotedField = false;
                    }
                    currentFieldContentStart = matchEnd + 1;
                }

                if (result) {
                    break;
                }
            }

            return result;
        }

        @Override
        public int start() {
            return matchStart + 1;
        }

        @Override
        public int end() {
            return matchEnd;
        }

        @Override
        public String group() {
            return null;
        }
    }


    /**
     * Checks whether the previous position has a quote that is not escaped
     */
    public static boolean isPrecededByEffectiveQuote(CharSequence cs, int offset, int minOffset, char quoteChar, char escapeChar) {
        boolean result = isEffectiveQuoteBwd(cs, offset - 1, minOffset, quoteChar, escapeChar);
        return result;
    }

    /**
     * Determine whether the next character is an effective quote.
     *
     * ."" -&gt; not an effective quote because it is an escaped quote symbol
     * Any odd number of "" implies an effective quote,
     */
    public static boolean isFollowedByEffectiveQuote(CharSequence cs, int offset, char quoteChar, char escapeChar) {
        boolean result = isEffectiveQuoteFwd(cs, offset + 1, quoteChar, escapeChar);
        return result;
    }

    public static boolean isEffectiveQuoteFwd(CharSequence cs, int offset, char quoteChar, char escapeChar) {
        boolean result = false;
        int n = cs.length();
        int i = offset;
        if (i < n) {
            char c = cs.charAt(i);
            if (c == quoteChar) {
                ++i;
                if (quoteChar == escapeChar) {
                    for (; i < n && cs.charAt(i) == quoteChar; ++i) { }
                    int delta = i - offset;
                    // An odd number of quote chars implies an effective quote
                    result = (delta & 1) == 1;
                } else {
                    result = true;
                }
            }
        }
        return result;
    }

    public static boolean isEffectiveQuoteBwd(CharSequence cs, int offset, int minOffset, char quoteChar, char escapeChar) {
        boolean result = false;
        int i = offset;
        if (i >= minOffset) {
            char c = cs.charAt(i);
            if (c == quoteChar) {
                --i;
                for (; i >= minOffset && cs.charAt(i) == escapeChar; --i) { }
                int delta = i - offset;
                result = (delta & 1) == 1;
            }
        }
        return result;
    }


    public static void main(String[] args) {
        // Note: Consider the pattern: ,""",
        // Assuming that the first colon starts a field the trailing two double quotes are considered an escaped double quote

        // Note: The min offset must be the char after the field-quoting double quote - i.e. in the following couple cases 3 (rather than 2)
        System.out.println(isPrecededByEffectiveQuote("a,\"\"\",", 6, 3, '"', '"')); // false
        System.out.println(isPrecededByEffectiveQuote("a,\"\"\"\",", 6, 3, '"', '"')); // true

        System.out.println(isFollowedByEffectiveQuote(".\"", 0, '"', '"')); // true
        System.out.println(isFollowedByEffectiveQuote("a.\"\"", 1, '"', '"')); // false
        System.out.println(isPrecededByEffectiveQuote("\"\".", 2, 0, '"', '"')); // false
        System.out.println(isPrecededByEffectiveQuote("\".", 1, 0, '"', '"')); // true
    }
}