package net.sansa_stack.hadoop;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.opencsv.exceptions.CsvException;
import net.sansa_stack.hadoop.parser.csv.FileInputFormatCsv;
import net.sansa_stack.hadoop.parser.csv.RecordReaderCsv;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class RecordReaderCsvTestBase {

    private static final Logger logger = LoggerFactory.getLogger(RecordReaderCsvTestBase.class);

    public static List<Object[]> createParameters(Map<String, Range<Integer>> fileToNumSplits) {

        // Post process the map into junit params by enumerating the ranges
        // and creating a test case for each obtained value
        List<Object[]> result = fileToNumSplits.entrySet().stream()
                .flatMap(e -> ContiguousSet.create(e.getValue(), DiscreteDomain.integers()).stream()
                        .map(numSplits -> new Object[]{e.getKey(), numSplits}))
                .collect(Collectors.toList());

        return result;
    }

    protected String file;
    protected int numSplits;

    public RecordReaderCsvTestBase(String file, int numSplits) {
        this.file = file;
        this.numSplits = numSplits;
    }

    // protected abstract InputFormat<?, T> createInputFormat();

    /** Override this for custom hadoop configuration */
    protected void configureHadoop(Configuration conf) {};

    protected CSVParser newCsvParser(Reader reader) throws IOException {
        return new CSVParser(reader, CSVFormat.EXCEL);
    }

    @Test
    public void test() throws IOException, InterruptedException, CompressorException, CsvException {

        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");
        conf.set(RecordReaderCsv.RECORD_MAXLENGTH_KEY, "1000000");
        conf.set(RecordReaderCsv.RECORD_PROBECOUNT_KEY, "300");

        configureHadoop(conf);

        Path referencePath = Paths.get(file).toAbsolutePath();
        Path testPath = referencePath;

        List<List<String>> expected = new ArrayList<>();

        try (CSVParser csvParser = newCsvParser(new InputStreamReader(RecordReaderJsonArrayTestBase.autoDecode(Files.newInputStream(referencePath))))) {
            Iterator<CSVRecord> it = csvParser.iterator();
            while (it.hasNext()) {
                CSVRecord csvRecord = it.next();
                List<String> row = csvRecord.toList();
                expected.add(row);
            }
        }

        long fileLengthTotal = Files.size(testPath);


        Job job = Job.getInstance(conf);
        // TrigFileInputFormat inputFormat = new TrigFileInputFormat();
        InputFormat<?, List<String>> inputFormat = new FileInputFormatCsv();

        // add input path of the file
        org.apache.hadoop.fs.Path testHadoopPath = new org.apache.hadoop.fs.Path(testPath.toString());
        FileInputFormat.addInputPath(job, testHadoopPath);

        // call once to compute the prefixes
        // inputFormat.getSplits(job);

        List<List<String>> actual = new ArrayList<>();
        RecordReaderRdfTestBase.testSplit(job, inputFormat, testHadoopPath, fileLengthTotal, numSplits)
                .forEach(actual::add);


        Assert.assertEquals(expected, actual);

    }
}
