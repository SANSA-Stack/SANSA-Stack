package net.sansa_stack.hadoop.format.univocity.csv.csv;

import java.io.InputStream;
import java.util.concurrent.Callable;

import org.aksw.commons.model.csvw.univocity.UnivocityParserFactory;
import org.aksw.commons.rx.util.FlowableEx;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.common.record.Record;

import io.reactivex.rxjava3.core.Flowable;

public class UnivocityRxUtils {

    /** Configure univocity parser */
    /*
    public static void configureFromDialect(CommonParserSettings<?> settings, Dialect dialect) {
        // Create a copy of the settings
        DialectMutable d = JsonHadoopBridge.merge(
                new DialectMutableForwardingJackson<>(new DialectMutableImpl()),
                new DialectForwardingJackson<>(dialect));

        // CsvwLib.buildEffectiveModel(d, d);

        CsvwUnivocityUtils.configure(d, settings);
        // CsvwLib.buildEffectiveModel(dialect, d);
    }
     */


    /**
     * Create a flowable to a CSV file via hadoop. Allows for retrieval of headers.
     *
     * @param path A path (string) to a csv file
     * @param fileSystem The hadoop filesystem
     * @param parserFactory A factory for creating parsers over an input stream
     * @return A cold flowable over the csv data
     */
    public static Flowable<Record> readCsvRecords(String path, FileSystem fileSystem, UnivocityParserFactory parserFactory) {
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
    public static Flowable<Record> readCsvRecords(Path path, FileSystem fileSystem, UnivocityParserFactory parserFactory) {
        return readCsvRecords(() -> fileSystem.open(path), parserFactory);
    }

    public static Flowable<String[]> readCsvRows(Path path, FileSystem fileSystem, UnivocityParserFactory parserFactory) {
        return readCsvRecords(path, fileSystem, parserFactory).map(Record::getValues);
    }

    public static Flowable<String[]> readCsvRows(String path, FileSystem fileSystem, UnivocityParserFactory parserFactory) {
        return readCsvRecords(path, fileSystem, parserFactory).map(Record::getValues);
    }

    /**
     * Create a flowable to a CSV file from a supplier of input streams
     *
     * @param inSupp A supplier of input streams
     * @param parserFactory A factory for creating parsers over an input stream
     * @return A cold flowable over the csv data
     */
    public static Flowable<Record> readCsvRecords(Callable<? extends InputStream> inSupp, UnivocityParserFactory parserFactory) {
        return FlowableEx.fromEnumerableResource(
                () -> parserFactory.newInputStreamReader(inSupp.call()),
                reader -> { AbstractParser<?> r = parserFactory.newParser(); r.beginParsing(reader); return r; },
                csvParser -> csvParser.parseNextRecord(),
                (in, csvParser) -> {
                    if (in != null) {
                        in.close();
                    }
                });
    }
}
