package net.sansa_stack.hadoop;

import com.google.common.collect.Range;
import net.sansa_stack.hadoop.format.commons_csv.csv.CsvUtils;
import net.sansa_stack.hadoop.format.commons_csv.csv.FileInputFormatCsv;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class RecordReaderCommonsCsvTestsFast
        extends RecordReaderCsvTestBase<List> {

    @Override
    protected InputFormat getInputFormat() {
        return new FileInputFormatCsv();
    }

    @Override
    protected List<String> recordToList(List row) {
        return (List<String>)row;
    }

    @Override
    protected List<List<String>> parseConventional(Path path) {
        return CsvUtils.readCsvRecords(() ->
                RecordReaderJsonArrayTestBase.autoDecode(Files.newInputStream(path)), CSVFormat.EXCEL)
                .map(CSVRecord::toList).toList().blockingGet();
    }

    /**
     * Test case parameters
     */
    @Parameterized.Parameters(name = "{index}: file {0} with {1} splits")
    public static Iterable<Object[]> data() {
        // The map of test cases:
        // Each file is mapped to the number of  min splits and max splits(both inclusive)
        Map<String, Range<Integer>> map = new LinkedHashMap<>();

        map.put("src/test/resources/bio2rdf_sparql_logs_01-2019_to_07-2021.head10000.csv.bz2",
                Range.closed(1, 10));

        map.put("src/test/resources/bio2rdf_sparql_logs_processed_01-2019_to_07-2021.head10000.csv.bz2",
                Range.closed(1, 10));


//        map.put("src/test/resources/test-data.json.bz2",
//                Range.closed(1, 5));

        return createParameters(map);
    }

    public RecordReaderCommonsCsvTestsFast(String file, int numSplits) {
        super(file, numSplits);
    }
}