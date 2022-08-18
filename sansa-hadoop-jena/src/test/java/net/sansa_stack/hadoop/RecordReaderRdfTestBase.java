package net.sansa_stack.hadoop;

import com.google.common.collect.Iterators;
import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.format.jena.trig.RecordReaderRdfTrigDataset;
import net.sansa_stack.hadoop.util.FileSplitUtils;
import org.aksw.jenax.arq.dataset.orderaware.DatasetFactoryEx;
import org.aksw.jenax.sparql.query.rx.RDFDataMgrEx;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

/**
 * Test cases for the {@link RecordReaderRdfTrigDataset}:
 * A given set of test datasets (represented as trig files) is first split into a configurable
 * number of splits from which the overall graph is then reassembled.
 * The reassembled dataset must match the one of the original file.
 *
 * @author Lorenz Buehmann
 * @author Claus Stadler
 */
public abstract class RecordReaderRdfTestBase<T> {

    private static final Logger logger = LoggerFactory.getLogger(RecordReaderRdfTestBase.class);

    protected String file;
    protected int numSplits;

    public RecordReaderRdfTestBase(String file, int numSplits) {
        this.file = file;
        this.numSplits = numSplits;
    }

    protected abstract InputFormat<?, T> createInputFormat();
    protected abstract void accumulate(Dataset target, T contrib);

    /** Override this for custom hadoop configuration */
    protected void configureHadoop(Configuration conf) {}

    /**
     * Override this to use a different Dataset implementation
     * such as {@link DatasetFactoryEx#createInsertOrderPreservingDataset()}
     */
    protected Dataset createDataset() {
        return DatasetFactory.create();
    }

    protected void assertIsIsomorphic(Dataset expected, Dataset actual) {
        boolean isIso = DatasetCompareUtils.isIsomorphic(
                expected, actual, true,
                System.err, ResultSetLang.RS_TSV);

        Assert.assertTrue("Datasets were not isomorphic - see output above", isIso);
    }


    @Test
    public void test() throws IOException, InterruptedException {

        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");
        conf.set(RecordReaderRdfTrigDataset.RECORD_MAXLENGTH_KEY, "10000");
        conf.set(RecordReaderRdfTrigDataset.RECORD_PROBECOUNT_KEY, "2");

        configureHadoop(conf);

        Path referencePath = Paths.get(file).toAbsolutePath();
        Path testPath = referencePath;

        Dataset expectedDataset = createDataset();
        // RDFDataMgr.read(expectedDataset, referencePath.toString());
        RDFDataMgrEx.readAsGiven(expectedDataset, referencePath.toString());

        long fileLengthTotal = Files.size(testPath);


        Job job = Job.getInstance(conf);
        // TrigFileInputFormat inputFormat = new TrigFileInputFormat();
        InputFormat<?, T> inputFormat = createInputFormat();

        // add input path of the file
        org.apache.hadoop.fs.Path testHadoopPath = new org.apache.hadoop.fs.Path(testPath.toString());
        FileInputFormat.addInputPath(job, testHadoopPath);

        // call once to compute the prefixes
        inputFormat.getSplits(job);

        /**
         * Testing n splits by manually created RecordReader
         * Ensure to start the loop from 1 for full testing. Takes quite long.
         */
        // compare with target dataset
        Dataset actualDataset = createDataset();

        try (Stream<T> stream = testSplit(job, inputFormat, testHadoopPath, fileLengthTotal, numSplits)) {
            stream.forEach(record -> accumulate(actualDataset, record));
        }

        logger.info(String.format("Named graph counts expected/actual: %d/%d",
                Iterators.size(expectedDataset.listNames()),
                Iterators.size(actualDataset.listNames())));

        logger.info(String.format("Quad counts expected/actual: %d/%d",
                Iterators.size(expectedDataset.asDatasetGraph().find()),
                Iterators.size(actualDataset.asDatasetGraph().find())));

        // compareDatasets(expectedDataset, actualDataset);
        assertIsIsomorphic(expectedDataset, actualDataset);

    }

    //test("parsing Trig file provided by $i splits") {

    /**
     * Create a sequential stream of all records covering all consecutive splits in order
     */
    public static <T> Stream<T> testSplit(
            Job job,
            InputFormat<?, T> inputFormat,
            org.apache.hadoop.fs.Path testHadoopPath,
            long fileTotalLength,
            int numSplits
    ) throws IOException, InterruptedException {
        List<InputSplit> splits = FileSplitUtils.listFileSplits(testHadoopPath, fileTotalLength, numSplits);

        return splits.stream().flatMap(split -> FileSplitUtils.createFlow(job, inputFormat, split));
    }


    /**
     * Testing n splits by RecordReader created from Inputformat (incl. parsed prefixes)
     */
  /*
  test("multiple splits parsed using InputFormat") {

    val job = Job.getInstance(conf)
    val inputFormat = new TrigFileInputFormat()

    // add input path of the file
    FileInputFormat.addInputPath(job, new Path(testFile.getAbsolutePath))

    // get splits from InputFormat
    val splits = inputFormat.getSplits(job)

    splits.asScala.foreach { split =>
      // create the reader
      val reader = inputFormat.createRecordReader(split, new TaskAttemptContextImpl(conf, new TaskAttemptID()))

      // read all records in split
      val ds = consumeRecords(reader)

      // compare with target dataset
      compareDatasets(targetDataset, ds)
    }
  }
  */


    /*
    public Dataset consumeRecords(RecordReader<?, Dataset> reader) throws IOException, InterruptedException {
        Dataset result = DatasetFactory.create();
        // val actual = new mutable.ListBuffer[(LongWritable, Dataset)]()
        // var counter = 0;
        while (reader.nextKeyValue()) {
            // LongWritable k = reader.getCurrentKey();
            Dataset v = reader.getCurrentValue();
            // val item = (k, v)
            // actual += item
            // println(s"Dataset ${k.get()}:")
            // RDFDataMgr.write(System.out, v, RDFFormat.TRIG_PRETTY)


            DatasetGraphUtils.addAll(result.asDatasetGraph(), v.asDatasetGraph());
            // counter += 1
        }

        // println(s"Counted $counter records")

        // merge to single dataset
        // actual.map(_._2).foldLeft(DatasetFactory.create())((ds1, ds2) => DatasetLib.union(ds1, ds2))
        return result;
    }

     */
}