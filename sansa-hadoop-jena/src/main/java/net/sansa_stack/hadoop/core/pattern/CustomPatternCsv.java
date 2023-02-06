package net.sansa_stack.hadoop.core.pattern;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.aksw.commons.model.csvw.domain.api.Dialect;
import org.aksw.commons.model.csvw.domain.api.DialectMutable;
import org.aksw.commons.model.csvw.domain.impl.CsvwLib;
import org.aksw.commons.model.csvw.domain.impl.DialectMutableImpl;

import net.sansa_stack.hadoop.core.pattern.CustomPatternReplay.CustomMatcherReplay;

public class CustomPatternCsv
    implements CustomPattern
{
    protected Dialect dialect;
    protected CustomPattern fieldSeparatorAndNewlinePattern;
    protected int multilineFieldMaxLines;


    // FIXME cellMaxLength is not yet wired up
    protected int cellMaxLength;

    public static Pattern createPattern(Dialect dialect) {
        List<String> ts = dialect.getLineTerminatorList();
        if (ts == null || ts.isEmpty()) {
            ts = CsvwLib.DFT_LINE_TERMINATORS; // null; // \r?\n\r?
        }

        String fieldSeparator = Optional.ofNullable(dialect.getDelimiter()).orElse(",");

        String patternStr =
            "(?<" + CsvwLib.LINE_TERMINATOR_KEY + ">" + ts.stream().map(x -> "(" + Pattern.quote(x) + ")").collect(Collectors.joining("|")) + ")|" +
            "(?<" + CsvwLib.END_OF_DATA_KEY + ">$)|" +
            "(?<" + CsvwLib.FIELD_SEPARATOR_KEY + ">" + Pattern.quote(fieldSeparator) + ")";

        // System.out.println(patternStr);
        return Pattern.compile(patternStr, Pattern.MULTILINE | Pattern.DOTALL);
    }

    public static CustomPattern create(int multilineFieldMaxLines) {
        return create(null, multilineFieldMaxLines, Integer.MAX_VALUE);
    }

    public static CustomPattern create(Dialect dialect, int multilineFieldMaxLines, int cellMaxLength) {
        DialectMutable copy = new DialectMutableImpl();
        dialect.copyInto(copy, true);
        CsvwLib.buildEffectiveModel(dialect, copy);

        return new CustomPatternCsv(dialect, multilineFieldMaxLines, cellMaxLength);
    }

    protected CustomPatternCsv(Dialect dialect, int multilineFieldMaxLines, int cellMaxLength) {
        super();
        this.dialect = dialect;
        this.fieldSeparatorAndNewlinePattern = new CustomPatternJava(createPattern(dialect));
        this.multilineFieldMaxLines = multilineFieldMaxLines;
        this.cellMaxLength = cellMaxLength;
    }

    @Override
    public CustomMatcherCsv2 matcher(CharSequence charSequence) {
        return new CustomMatcherCsv2(charSequence);
    }


    public static class MatchRegion {
        protected long start;
        protected long end;

        public MatchRegion(long start, long end) {
            super();
            this.start = start;
            this.end = end;
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }
    }

    public static class Field
        extends MatchRegion
    {
        protected CharSequence prefix; // Typically an empty (zero-length) sequence
        protected CharSequence suffix; // Typically the field separator (,) or newline
        protected CharSequence content;

        public Field(long start, long end, CharSequence prefix, CharSequence suffix, CharSequence content) {
            super(start, end);
            this.prefix = prefix;
            this.suffix = suffix;
            this.content = content;
        }
        public CharSequence getPrefix() {
            return prefix;
        }
        public CharSequence getSuffix() {
            return suffix;
        }
        public CharSequence getContent() {
            return content;
        }
    }

    public static class Row
        extends MatchRegion
    {
        protected List<Field> fields;
        int columnCount;

        public Row(long start, long end, List<Field> fields, int columnCount) {
            super(start, end);
            this.fields = fields;
            this.columnCount = columnCount;
        }
    }

    public static class MatchState {
        protected int currentFieldContentStart;
        protected boolean isInQuotedField;
        protected List<Row> rows;

        public MatchState(boolean isInQuotedField, List<Row> rows) {
            super();
            this.isInQuotedField = isInQuotedField;
            this.rows = rows;
        }

        public boolean isInQuotedField() {
            return isInQuotedField;
        }

        public List<Row> getRows() {
            return rows;
        }
    }

    public class CustomMatcherCsv2
        extends CustomMatcherBase
    {
        /** Matcher to move to the next field separator or newline */
        protected CustomMatcherReplay fieldSeparatorAndNewlineMatcher;

        /** Number of unexpected quotations */
        protected int quoteErrorCount = 0;

        protected List<CharSequence> lastMatchedFields = new ArrayList<>();

        public CustomMatcherCsv2(CharSequence charSequence) {
            super(charSequence);
            fieldSeparatorAndNewlineMatcher = CustomPatternReplay.wrap(fieldSeparatorAndNewlinePattern).matcher(charSequence);
        }

        /** Start of field content (excluding a possible leading a quote) */
        protected int currentFieldContentStart = 0;
        protected Boolean isInQuotedField = null;

        protected int currentLineCount;
        // protected List<MatchState> states;

        // protected long matchEnd = -1;
        protected int lastRowEnd = 0;
        protected int lastRowStart = 0;

        public List<CharSequence> getLastMatchedFields() {
            return lastMatchedFields;
        }

        public int getQuoteErrorCount() {
            return quoteErrorCount;
        }

        @Override
        public boolean find() {
            boolean result;

            // TODO Skip the first row because it might be incomplete
            do {
                result = findNext();
            } while (result && lastRowStart == 0);

            return result;
        }

        public boolean findNext() {
            boolean result = false;
            char quoteChar = CsvwLib.expectAtMostOneChar("quote char", dialect.getQuoteChar(), '"');
            char escapeChar = CsvwLib.expectAtMostOneChar("escape char", dialect.getQuoteEscapeChar(), '"');

            lastMatchedFields.clear();
            quoteErrorCount = 0;

            if (pos < 0) {
                return false;
            }

            if (isInQuotedField == null) {
                autoDetectStartInQuotedField(this);
            }

            int lastMatchEnd = lastRowEnd;

            while (fieldSeparatorAndNewlineMatcher.find()) {
                int matchStart = fieldSeparatorAndNewlineMatcher.start();
                int matchEnd = fieldSeparatorAndNewlineMatcher.end();

                boolean isEnd = fieldSeparatorAndNewlineMatcher.start(CsvwLib.END_OF_DATA_KEY) >= 0;

                if (fieldSeparatorAndNewlineMatcher.start(CsvwLib.LINE_TERMINATOR_KEY) >= 0 || isEnd) {
                    // If we are in a multiline field which is NOT terminated before the newline then continue
                    // otherwise return the position after the line separator as the next row
                    if (isPrecededByEffectiveQuote(charSequence, matchStart, currentFieldContentStart, quoteChar, escapeChar)) {
                        if (isInQuotedField) {
                            // End of field
                            isInQuotedField = false;
                            result = true;
                        } else {
                            // There is a quote before a delimiter such as [",] without a multiline field
                            // it may be a trailing quote in a non-quoted field but most likely either
                            // its an error - either a data issue or our assumption being in a multline-field was wrong
                            ++quoteErrorCount;
                            result = true; // Return a match that has a quote error count
                            // continue;
                        }
                    }
                    if (isInQuotedField) {
                        if (isEnd) {
                            ++quoteErrorCount;
                            result = true;
                        } else {
                            continue;
                        }
                    }

                    if (currentLineCount > multilineFieldMaxLines) {
                        ++quoteErrorCount;
                        result = true;
                    }

                    result = true;
                } else if (fieldSeparatorAndNewlineMatcher.start(CsvwLib.FIELD_SEPARATOR_KEY) >= 0) {
                    if (isPrecededByEffectiveQuote(charSequence, matchStart, currentFieldContentStart, quoteChar, escapeChar)) {
                        if (!isInQuotedField) {
                            ++quoteErrorCount;
                        }
                        isInQuotedField = false;
                    }
                    if (isInQuotedField) {
                        continue;
                    }
                } else {
                    throw new IllegalStateException("should not happen");
                }

                // lastMatchedFields.add(charSequence.subSequence(lastMatchEnd, matchStart));
                lastMatchedFields.add("[" + lastMatchEnd + ", " + matchStart + "]");
                lastMatchEnd = matchEnd;

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
                    lastRowStart = lastRowEnd;
                    lastRowEnd = matchEnd;
                    break;
                }
            }

            if (result) {
                System.out.println("[" + lastMatchedFields.stream().collect(Collectors.joining("||")) + "]");
                System.out.println();
            }

            return result;
        }

        public void setInQuotedField(boolean onOrOff) {
            this.isInQuotedField = onOrOff;
        }

        public boolean IsInQuotedField() {
            return isInQuotedField;
        }

        public void reset() {
            lastRowStart = 0;
            lastRowEnd = 0;
            currentFieldContentStart = 0;
            fieldSeparatorAndNewlineMatcher.reset();
        }

        @Override
        public int start() {
            return lastRowStart; // lastRowEnd;
        }

        @Override
        public int end() {
            return lastRowEnd;
        }

        @Override
        public String group() {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public int start(String name) {
            return fieldSeparatorAndNewlineMatcher.start(name);
        }

        @Override
        public int end(String name) {
            return fieldSeparatorAndNewlineMatcher.end(name);
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


    public static void mainX(String[] args) {
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


    /**
     * Attempt to auto-detect whether the csv row matcher is positioned inside of a quoted field.
     * The matcher is reset and invoked with either assumption (inside of a quoted field and outside of it).
     * The assumption that leads to a consistent sample of rows will take effect.
     * Consistent means that all rows have the same length and that no quote errors are encountered.
     *
     * @param matcher
     */
    public static void autoDetectStartInQuotedField(CustomMatcherCsv2 matcher) {
        int rowProbeCount = 10;

        int verdict = -1; // -1 = no match found, 0 = unquoted, 1 = quoted, 2 = failed to decide
        nextAssumption: for (int r = 0; r <= 1; ++r) {
            boolean quoted = r % 2 == 1;
            matcher.setInQuotedField(quoted);
            matcher.reset();
            int lastFieldCount = -1;
            int i;
            for (i = 0; i < rowProbeCount; ++i) {
                if (matcher.find()) {
                    // Skip the first row
                    // TODO Add toggle
//                    if (i == 0) {
//                        continue;
//                    }

                    int n = matcher.getLastMatchedFields().size();
                    int quoteErrorCount = matcher.getQuoteErrorCount();
                    System.out.println("quoteErrorCount: " + quoteErrorCount);
                    if (lastFieldCount == -1) {
                        lastFieldCount = n;
                    }

                    if (lastFieldCount != n || quoteErrorCount > 0) {
                        verdict = 2;
                        continue nextAssumption;
                    }
                } else {
                    break;
                }
            }
            // If we found at least 1 row we can set the assumption as the result
            if (i > 0) {
                verdict = r;
                break;
            }
        }

        if (verdict == 2) {
            throw new IllegalStateException("Could not decide whether position is inside or outside of a quoted field because either assumption led to quote errors and/or sample of rows having varying lengths");
        }

        matcher.reset();
        matcher.setInQuotedField(verdict == 1);
    }
}