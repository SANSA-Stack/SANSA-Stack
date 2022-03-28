package net.sansa_stack.hadoop;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import net.sansa_stack.hadoop.format.gson.json.FileInputFormatJsonArray;
import net.sansa_stack.hadoop.format.gson.json.RecordReaderJsonArray;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test cases for the {@link RecordReaderJsonArray}:
 * A given set of test datasets (represented as trig files) is first split into a configurable
 * number of splits from which the overall graph is then reassembled.
 * The reassembled dataset must match the one of the original file.
 *
 * @author Lorenz Buehmann
 * @author Claus Stadler
 */
public abstract class RecordReaderJsonArrayTestBase {

    private static final Logger logger = LoggerFactory.getLogger(RecordReaderJsonArrayTestBase.class);

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

    public RecordReaderJsonArrayTestBase(String file, int numSplits) {
        this.file = file;
        this.numSplits = numSplits;
    }

    // protected abstract InputFormat<?, T> createInputFormat();

    /** Override this for custom hadoop configuration */
    protected void configureHadoop(Configuration conf) {};

    /**
     * Conventional auto-decoding using commons-compress.
     * Returns the given stream as-is if no decoder is applicable
     */
    public static InputStream autoDecode(InputStream in) {

        if (!in.markSupported()) {
            in = new BufferedInputStream(in);
        }

        InputStream result;
        try {
            result = CompressorStreamFactory.getSingleton().createCompressorInputStream(in);
        } catch (CompressorException e) {
            result = in;
        }
        return result;
    }

    @Test
    public void test() throws IOException, InterruptedException, CompressorException {

        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");
        conf.set(RecordReaderJsonArray.RECORD_MAXLENGTH_KEY, "1000000");
        conf.set(RecordReaderJsonArray.RECORD_PROBECOUNT_KEY, "300");

        configureHadoop(conf);

        Path referencePath = Paths.get(file).toAbsolutePath();
        Path testPath = referencePath;

        JsonElement expected;
        Gson gson = new Gson();

        try (Reader reader = new InputStreamReader(autoDecode(Files.newInputStream(referencePath)))) {
            expected = gson.fromJson(reader, JsonElement.class);
        }
        ;


        long fileLengthTotal = Files.size(testPath);


        Job job = Job.getInstance(conf);
        // TrigFileInputFormat inputFormat = new TrigFileInputFormat();
        InputFormat<?, JsonElement> inputFormat = new FileInputFormatJsonArray();

        // add input path of the file
        org.apache.hadoop.fs.Path testHadoopPath = new org.apache.hadoop.fs.Path(testPath.toString());
        FileInputFormat.addInputPath(job, testHadoopPath);

        // call once to compute the prefixes
        // inputFormat.getSplits(job);

        JsonArray actual = new JsonArray();
        RecordReaderRdfTestBase.testSplit(job, inputFormat, testHadoopPath, fileLengthTotal, numSplits)
                .forEach(actual::add);


        Assert.assertEquals(expected, actual);

                // .forEach(record -> accumulate(actualDataset, record));

        /**
         * Testing n splits by manually created RecordReader
         * Ensure to start the loop from 1 for full testing. Takes quite long.
         */
        // compare with target dataset
        /*
        Dataset actualDataset = createDataset();

        testSplit(job, inputFormat, testHadoopPath, fileLengthTotal, numSplits)
                .forEach(record -> accumulate(actualDataset, record));
        logger.info(String.format("Named graph counts expected/actual: %d/%d",
                Iterators.size(expectedDataset.listNames()),
                Iterators.size(actualDataset.listNames())));

        logger.info(String.format("Quad counts expected/actual: %d/%d",
                Iterators.size(expectedDataset.asDatasetGraph().find()),
                Iterators.size(actualDataset.asDatasetGraph().find())));

        // compareDatasets(expectedDataset, actualDataset);
        assertIsIsomorphic(expectedDataset, actualDataset);
         */

    }
}