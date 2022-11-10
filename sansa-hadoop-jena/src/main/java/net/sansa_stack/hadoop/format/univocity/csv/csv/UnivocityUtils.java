package net.sansa_stack.hadoop.format.univocity.csv.csv;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.aksw.commons.collections.utils.StreamUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.common.CommonParserSettings;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class UnivocityUtils {

    public static boolean isDetectionNeeded(CsvParserSettings settings) {
        boolean result = settings.isLineSeparatorDetectionEnabled()
                || settings.isDelimiterDetectionEnabled()
                || settings.isQuoteDetectionEnabled();

        return result;
    }

    public static boolean isDetectionNeeded(CommonParserSettings<?> settings) {
        boolean result = settings.isLineSeparatorDetectionEnabled();
        return result;
    }

    public static CsvFormat detectFormat(CsvParser parser, Supplier<Reader> readerSupp) throws IOException {
        CsvFormat result;
        try (Reader reader = readerSupp.get()) {
             parser.beginParsing(reader);
             result = parser.getDetectedFormat();
        } finally {
             parser.stopParsing();
        }
        return result;
    }

    /**
     * Create a flowable to a CSV file via hadoop. Allows for retrieval of headers.
     *
     * @param path A path (string) to a csv file
     * @param fileSystem The hadoop filesystem
     * @param parserFactory A factory for creating parsers over an input stream
     * @return A cold flowable over the csv data
     */
    public static Stream<Record> readCsvRecords(String path, FileSystem fileSystem, UnivocityParserFactory parserFactory) {
        return readCsvRecords(new Path(path), fileSystem, parserFactory);
    }

    /**
     * Create a flowable to a CSV file via hadoop. Allows for retrieval of headers.
     *
     * @param path A path to a csv file
     * @param fileSystem The hadoop filesystem
     * @param parserFactory A factory for creating parsers over an input stream
     * @return A cold flowable over the csv data
     */
    public static Stream<Record> readCsvRecords(Path path, FileSystem fileSystem, UnivocityParserFactory parserFactory) {
        return readCsvElements(() -> fileSystem.open(path), parserFactory, AbstractParser::parseNextRecord);
    }

    public static Stream<String[]> readCsvRows(Path path, FileSystem fileSystem, UnivocityParserFactory parserFactory) {
        return readCsvElements(path, fileSystem, parserFactory, AbstractParser::parseNext);
    }

    public static <T> Stream<T> readCsvElements(Path path, FileSystem fileSystem, UnivocityParserFactory parserFactory, Function<AbstractParser<?>, T> extractElement) {
        return readCsvElements(() -> fileSystem.open(path), parserFactory, extractElement);
    }

    /**
     * Create a stream to a CSV file from a supplier of input streams
     *
     * @param inSupp A supplier of input streams
     * @param parserFactory A factory for creating parsers over an input stream
     * @return A cold flowable over the csv data
     */
    public static <T> Stream<T> readCsvElements(
            Callable<? extends InputStream> inSupp,
            UnivocityParserFactory parserFactory,
            Function<AbstractParser<?>, T> extractElement) {
        return StreamUtils.fromEnumerableResource(
                () -> parserFactory.newInputStreamReader(inSupp.call()),
                reader -> { AbstractParser<?> r = parserFactory.newParser(); r.beginParsing(reader); return r; },
                csvParser -> extractElement.apply(csvParser),
                (record, parser) -> record != null,
                (in, csvParser) -> {
                    if (in != null) {
                        in.close();
                    }
                });
    }
}
