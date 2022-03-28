package net.sansa_stack.hadoop;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import net.sansa_stack.hadoop.format.commons_csv.csv.RecordReaderCsv;
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

public abstract class RecordReaderCsvTestBase<T> {

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

    protected abstract InputFormat<?, T> getInputFormat();
    protected abstract List<String> recordToList(T record);

    protected abstract List<List<String>> parseConventional(Path resource);

    @Test
    public void test() throws IOException, InterruptedException, CompressorException {

        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");
        conf.set(RecordReaderCsv.RECORD_MAXLENGTH_KEY, "1000000");
        conf.set(RecordReaderCsv.RECORD_PROBECOUNT_KEY, "200");

        configureHadoop(conf);

        Path referencePath = Paths.get(file).toAbsolutePath();
        Path testPath = referencePath;

        List<List<String>> expected = parseConventional(referencePath);

        int m = Math.min(2, expected.size());
        for (int i = 0; i < m; ++i) {
            List<String> row = expected.get(i);

            System.out.println("Row #" + i);
            int n = row.size();
            for (int j = 0; j < n; ++j) {
                System.out.println("Cell: " + row.get(j));
            }
        }

        long fileLengthTotal = Files.size(testPath);


        Job job = Job.getInstance(conf);

        InputFormat<?, T> inputFormat = getInputFormat();

        // add input path of the file
        org.apache.hadoop.fs.Path testHadoopPath = new org.apache.hadoop.fs.Path(testPath.toString());
        FileInputFormat.addInputPath(job, testHadoopPath);

        // call once to compute the prefixes
        // inputFormat.getSplits(job);

        List<List<String>> actual = new ArrayList<>();


        Throwable[] error = new Throwable[]{null};
        RecordReaderRdfTestBase.testSplit(job, inputFormat, testHadoopPath, fileLengthTotal, numSplits)
                .map(this::recordToList)
                .doOnError(t -> error[0] = t)
                .onErrorComplete()
                .forEach(actual::add);

        if (error[0] != null) {
            throw new RuntimeException(error[0]);
        }

        // Compare first line
        if (!expected.isEmpty() && !actual.isEmpty()) {
            Assert.assertEquals(expected.get(0), actual.get(0));
        }

        Assert.assertEquals(expected, actual);

    }
}
