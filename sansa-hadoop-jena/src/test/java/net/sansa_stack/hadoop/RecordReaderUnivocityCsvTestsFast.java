package net.sansa_stack.hadoop;

import com.google.common.collect.Range;
import com.univocity.parsers.common.record.Record;
import net.sansa_stack.hadoop.format.univocity.conf.UnivocityHadoopConf;
import net.sansa_stack.hadoop.format.univocity.csv.csv.UnivocityParserFactory;
import net.sansa_stack.hadoop.format.univocity.csv.csv.UnivocityUtils;
import net.sansa_stack.hadoop.format.univocity.csv.csv.FileInputFormatCsvUnivocity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class RecordReaderUnivocityCsvTestsFast
        extends RecordReaderCsvTestBase<String[]> {

    @Override
    protected InputFormat getInputFormat() {
        return new FileInputFormatCsvUnivocity();
    }

    @Override
    protected void configureHadoop(Configuration conf) {
        super.configureHadoop(conf);
        UnivocityHadoopConf csvConf = new UnivocityHadoopConf();
        // TODO What should be the default csv settings w.r.t. to headers?
        // Probably headers should be assumed
        csvConf.getDialect().setHeaderRowCount(1l);
        csvConf.getDialect().setHeader(true);
        FileInputFormatCsvUnivocity.setUnivocityConfig(conf, csvConf);
    }

    @Override
    protected List<String> recordToList(String[] row) {
        return Arrays.asList(row);
    }

    @Override
    protected List<List<String>> parseConventional(Path path) {

        UnivocityParserFactory parserFactory = UnivocityParserFactory.createDefault(true);

        return UnivocityUtils.readCsvRecords(() ->
                RecordReaderJsonArrayTestBase.autoDecode(Files.newInputStream(path)), parserFactory)
                .map(Record::getValues).map(Arrays::asList).toList().blockingGet();
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

    public RecordReaderUnivocityCsvTestsFast(String file, int numSplits) {
        super(file, numSplits);
    }
}