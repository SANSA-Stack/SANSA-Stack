package net.sansa_stack.hadoop.core.pattern;

import java.util.Optional;
import java.util.regex.Pattern;

import org.aksw.commons.model.csvw.domain.api.Dialect;
import org.aksw.commons.model.csvw.domain.impl.CsvwLib;

/** Adapter method to configure the CustomPatternCsv from a Csvw Dialect */
public class CustomPatternCsvFromCsvwOld {
    public static CustomPatternCsvOld.Config adapt(Dialect dialect, int maxColumnLength) {
        char quoteChar = CsvwLib.expectAtMostOneChar("quoteChar", dialect.getQuoteChar(), '"');
        char escapeChar = CsvwLib.expectAtMostOneChar("quoteEscapeChar", dialect.getQuoteEscapeChar(), '"');
        String delimiterPattern = Optional.ofNullable(dialect.getDelimiter()).orElse(Pattern.quote(","));
        String lineTerminatorPattern = Optional.ofNullable(dialect.getLineTerminators()).orElse("\r?\n\r?");
        return new CustomPatternCsvOld.Config(quoteChar, escapeChar, delimiterPattern, lineTerminatorPattern, maxColumnLength, 20);
    }
}
