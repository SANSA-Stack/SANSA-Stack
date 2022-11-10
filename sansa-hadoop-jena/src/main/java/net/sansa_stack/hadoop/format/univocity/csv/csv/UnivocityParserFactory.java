package net.sansa_stack.hadoop.format.univocity.csv.csv;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.aksw.commons.model.csvw.domain.api.Dialect;
import org.aksw.commons.model.csvw.domain.impl.CsvwLib;
import org.aksw.commons.model.csvw.univocity.CsvwUnivocityUtils;

import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.common.CommonParserSettings;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import net.sansa_stack.hadoop.format.univocity.conf.UnivocityHadoopConf;

/**
 * A helper class for setting up a univocity parser for an input stream.
 * Concretely captures tsv and csv configuration and also a charset
 * attribute.
 */
public class UnivocityParserFactory {
    protected boolean isCsv;
    protected CsvParserSettings csvSettings;
    protected TsvParserSettings tsvSettings;
    protected Charset charset;

    public UnivocityParserFactory(boolean isCsv, Charset charset, CsvParserSettings csvSettings, TsvParserSettings tsvSettings) {
        this.isCsv = isCsv;
        this.charset = charset;
        this.csvSettings = csvSettings;
        this.tsvSettings = tsvSettings;
    }

    public CsvParserSettings getCsvSettings() {
        return csvSettings;
    }

    public TsvParserSettings getTsvSettings() {
        return tsvSettings;
    }

    public static UnivocityParserFactory createDefault(Boolean skipHeaders) {
        CsvParserSettings defaultCsvSettings = new CsvParserSettings();
        applyDefaults(defaultCsvSettings, skipHeaders);

        TsvParserSettings defaultTsvSettings = new TsvParserSettings();
        applyDefaults(defaultTsvSettings, skipHeaders);

        return new UnivocityParserFactory(true, StandardCharsets.UTF_8, defaultCsvSettings, defaultTsvSettings);
    }

    public static void applyDefaults(CommonParserSettings settings, Boolean skipHeaders) {
        settings.setMaxCharsPerColumn(-1); //500000);
        settings.setAutoClosingEnabled(false);
        //settings.setLineSeparatorDetectionEnabled(true);
        settings.setLineSeparatorDetectionEnabled(false);
        settings.trimValues(false);
        settings.setHeaderExtractionEnabled(!Boolean.FALSE.equals(skipHeaders));

        // Concurrent read must be false or non-deterministic errors are likely occur!
        settings.setReadInputOnSeparateThread(false);
    }

    public UnivocityParserFactory configure(UnivocityHadoopConf conf) {
        UnivocityParserFactory result;
        Dialect dialect = conf.getDialect();
        Charset cs = CsvwLib.getEncoding(dialect, charset);

        // The conf only affects either csv or tsv settings but not both
        if (conf.isTabs()) {
            TsvParserSettings settings = tsvSettings.clone();
            CsvwUnivocityUtils.configureCommonSettings(settings, dialect);
            result = new UnivocityParserFactory(false, cs, csvSettings, settings);
        } else {
            CsvParserSettings settings = csvSettings.clone();
            CsvwUnivocityUtils.configureCsvFormat(settings.getFormat(), dialect);
            CsvwUnivocityUtils.configureCommonSettings(settings, dialect);
            CsvwUnivocityUtils.configureDetection(settings, dialect);
            result = new UnivocityParserFactory(true, cs, settings, tsvSettings);
        }

        return result;
    }

    public InputStreamReader newInputStreamReader(InputStream in) {
        return new InputStreamReader(in, charset);
    }

    public AbstractParser<?> newParser() {
        AbstractParser<?> result = isCsv
                ? new CsvParser(csvSettings)
                : new TsvParser(tsvSettings);
        return result;
    }
}
