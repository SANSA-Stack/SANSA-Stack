package net.sansa_stack.hadoop.format.commons_csv.csv;

import io.reactivex.rxjava3.core.Flowable;
import org.aksw.commons.rx.util.FlowableEx;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

public class CsvUtils {
    /**
     * Create a flowable to a CSV file via hadoop. Allows for retrieval of headers.
     *
     * @param path A path (string) to a csv file
     * @param fileSystem The hadoop filesystem
     * @param csvFormat The csv format for parsing the file
     * @return A cold flowable over the csv data
     */
    public static Flowable<CSVRecord> readCsvRecords(String path, FileSystem fileSystem, CSVFormat csvFormat) {
        return readCsvRecords(new Path(path), fileSystem, csvFormat);
    }

    /**
     * Create a flowable to a CSV file via hadoop. Allows for retrieval of headers.
     *
     * @param path A path to a csv file
     * @param fileSystem The hadoop filesystem
     * @param csvFormat The csv format for parsing the file
     * @return A cold flowable over the csv data
     */
    public static Flowable<CSVRecord> readCsvRecords(Path path, FileSystem fileSystem, CSVFormat csvFormat) {
        return readCsvRecords(() -> fileSystem.open(path), csvFormat);
    }

    public static Flowable<List<String>> readCsvRows(Path path, FileSystem fileSystem, CSVFormat csvFormat) {
        return readCsvRecords(path, fileSystem, csvFormat).map(CSVRecord::toList);
    }

    public static Flowable<List<String>> readCsvRows(String path, FileSystem fileSystem, CSVFormat csvFormat) {
        return readCsvRecords(path, fileSystem, csvFormat).map(CSVRecord::toList);
    }

    /**
     * Create a flowable to a CSV file from a supplier of input streams
     *
     * @param inSupp A supplier of input streams
     * @param csvFormat The csv format for parsing the file
     * @return A cold flowable over the csv data
     */
    public static Flowable<CSVRecord> readCsvRecords(Callable<? extends InputStream> inSupp, CSVFormat csvFormat) {
        return FlowableEx.fromIterableResource(
                () -> new InputStreamReader(inSupp.call()),
                reader -> new CSVParser(reader, csvFormat),
                csvParser -> csvParser.iterator(),
                (in, csvParser) -> {
                    if (csvParser != null) {
                        csvParser.close();
                    } else if (in != null) {
                        in.close();
                    }
                });
    }
}
