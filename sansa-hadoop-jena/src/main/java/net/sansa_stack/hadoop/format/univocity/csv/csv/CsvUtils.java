package net.sansa_stack.hadoop.format.univocity.csv.csv;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import io.reactivex.rxjava3.core.Flowable;
import org.aksw.commons.rx.util.FlowableEx;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;

public class CsvUtils {

    public static CsvParserSettings defaultSettings(boolean skipHeaders) {
        CsvParserSettings settings = new CsvParserSettings();
        settings.setMaxCharsPerColumn(500000);
        settings.setAutoClosingEnabled(false);
        settings.setLineSeparatorDetectionEnabled(true);
        settings.setHeaderExtractionEnabled(skipHeaders);
        settings.trimValues(false);
        settings.setEmptyValue("");
        settings.setNullValue("");
        return settings;
    }

    /**
     * Create a flowable to a CSV file via hadoop. Allows for retrieval of headers.
     *
     * @param path A path (string) to a csv file
     * @param fileSystem The hadoop filesystem
     * @param settings The csv format for parsing the file
     * @return A cold flowable over the csv data
     */
    public static Flowable<Record> readCsvRecords(String path, FileSystem fileSystem, CsvParserSettings settings) {
        return readCsvRecords(new Path(path), fileSystem, settings);
    }

    /**
     * Create a flowable to a CSV file via hadoop. Allows for retrieval of headers.
     *
     * @param path A path to a csv file
     * @param fileSystem The hadoop filesystem
     * @param settings The csv format for parsing the file
     * @return A cold flowable over the csv data
     */
    public static Flowable<Record> readCsvRecords(Path path, FileSystem fileSystem, CsvParserSettings settings) {
        return readCsvRecords(() -> fileSystem.open(path), settings);
    }

    public static Flowable<String[]> readCsvRows(Path path, FileSystem fileSystem, CsvParserSettings settings) {
        return readCsvRecords(path, fileSystem, settings).map(Record::getValues);
    }

    public static Flowable<String[]> readCsvRows(String path, FileSystem fileSystem, CsvParserSettings settings) {
        return readCsvRecords(path, fileSystem, settings).map(Record::getValues);
    }

    /**
     * Create a flowable to a CSV file from a supplier of input streams
     *
     * @param inSupp A supplier of input streams
     * @param settings The csv format for parsing the file
     * @return A cold flowable over the csv data
     */
    public static Flowable<Record> readCsvRecords(Callable<? extends InputStream> inSupp, CsvParserSettings settings) {
        return FlowableEx.fromEnumerableResource(
                () -> new InputStreamReader(inSupp.call()),
                reader -> { CsvParser r = new CsvParser(settings); r.beginParsing(reader); return r; },
                csvParser -> csvParser.parseNextRecord(),
                (in, csvParser) -> {
                    if (in != null) {
                        in.close();
                    }
                });
    }
}
