package net.sansa_stack.hadoop.core.pattern;

import org.aksw.commons.model.csvw.domain.api.Dialect;
import org.aksw.commons.model.csvw.domain.impl.CsvwLib;

import java.util.Optional;
import java.util.regex.Pattern;

/** Adapter method to configure the CustomPatternCsv from a Csvw Dialect */
public class CustomPatternCsvFromCsvw {

    public static CustomPatternCsv.Config adapt(Dialect dialect) {
        char quoteChar = CsvwLib.expectAtMostOneChar("quoteChar", dialect.getQuoteChar(), '"');
        char escapeChar = CsvwLib.expectAtMostOneChar("quoteEscapeChar", dialect.getQuoteEscapeChar(), '"');
        String delimiterPattern = Optional.ofNullable(dialect.getDelimiter()).orElse(Pattern.quote(","));
        String lineTerminatorPattern = Optional.ofNullable(dialect.getLineTerminators()).orElse("\r?\n\r?");
        return new CustomPatternCsv.Config(quoteChar, escapeChar, delimiterPattern, lineTerminatorPattern, 20);
    }

}
