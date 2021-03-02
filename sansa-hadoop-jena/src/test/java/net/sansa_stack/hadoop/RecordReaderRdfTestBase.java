package net.sansa_stack.hadoop;

import com.google.common.collect.*;
import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.jena.rdf.trig.RecordReaderTrigDataset;
import net.sansa_stack.hadoop.util.FileSplitUtils;
import org.aksw.jena_sparql_api.rx.DatasetFactoryEx;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
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
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test cases for the {@link RecordReaderTrigDataset}:
 * A given set of test datasets (represented as trig files) is first split into a configurable
 * number of splits from which the overall graph is then reassembled.
 * The reassembled dataset must match the one of the original file.
 *
 * @author Lorenz Buehmann
 * @author Claus Stadler
 */
public abstract class RecordReaderRdfTestBase<T> {

    private static final Logger logger = LoggerFactory.getLogger(RecordReaderRdfTestBase.class);

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

    public RecordReaderRdfTestBase(String file, int numSplits) {
        this.file = file;
        this.numSplits = numSplits;
    }

    protected abstract InputFormat<?, T> createInputFormat();
    protected abstract void accumulate(Dataset target, T contrib);

    /** Override this for custom hadoop configuration */
    protected void configureHadoop(Configuration conf) {};

    /**
     * Override this to use a different Dataset implementation
     * such as {@link DatasetFactoryEx#createInsertOrderPreservingDataset()}
     */
    protected Dataset createDataset() {
        return DatasetFactory.create();
    };

    protected void assertIsIsomorphic(Dataset expected, Dataset actual) {
        boolean isIso = DatasetCompareUtils.isIsomorphic(
                expected, actual, true,
                System.err, ResultSetLang.SPARQLResultSetTSV);

        Assert.assertTrue("Datasets were not isomoprhic - see output above", isIso);
    }


    @Test
    public void test() throws IOException, InterruptedException {

        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");
        conf.set(RecordReaderTrigDataset.RECORD_MAXLENGTH_KEY, "10000");
        conf.set(RecordReaderTrigDataset.RECORD_PROBECOUNT_KEY, "1");

        configureHadoop(conf);

        // val testFileName = "w3c_ex2.trig"
        // val referenceFileName = "nato-phonetic-alphabet-example.trig"
        // val testFileName = "nato-phonetic-alphabet-example.trig"
        // val testFileName = "nato-phonetic-alphabet-example.trig.bz2"

        // val referenceFile = new File("/home/raven/Projects/Data/Hobbit/hobbit-sensor-stream-150k.trig")
        // val testFile = new File("/home/raven/Projects/Data/Hobbit/hobbit-sensor-stream-150k.trig")
        // val testFile = new File("/home/raven/Projects/Eclipse/facete3-parent/version1/hobbit-sensor-stream-150k-events-data.trig.bz2")


        // val referenceFile new File(getClass.getClassLoader.getResource("/hobbit-sensor-stream-150k-events-data.trig.bz2").getPath)

        Path referencePath = Paths.get(file).toAbsolutePath();
        Path testPath = referencePath;
        // val testFile = new File(getClass.getClassLoader.getResource("/hobbit-sensor-stream-150k-events-data.trig.bz2").getPath)

        // read the target dataset
        // Dataset expectedDataset = DatasetFactory.create();
        //RDFDataMgr.read(expectedDataset, new BZip2CompressorInputStream(Files.newInputStream(referencePath)), Lang.TRIG);
        // Dataset expectedDataset = DatasetFactoryEx.createInsertOrderPreservingDataset();

        // Supplier<Dataset> datasetFactory = DatasetFactory::create;
        // Supplier<Dataset> datasetFactory = DatasetFactoryEx::createInsertOrderPreservingDataset;

        Dataset expectedDataset = createDataset();
        RDFDataMgr.read(expectedDataset, referencePath.toString());

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

    }

    //test("parsing Trig file provided by $i splits") {

    /**
     * Create a sequential stream of all records covering all consecutive splits in order
     */
    public static <T> Flowable<T> testSplit(
            Job job,
            InputFormat<?, T> inputFormat,
            org.apache.hadoop.fs.Path testHadoopPath,
            long fileTotalLength,
            int numSplits
    ) throws IOException, InterruptedException {
        List<InputSplit> splits = FileSplitUtils.generateFileSplits(testHadoopPath, fileTotalLength, numSplits);

        return Flowable.fromIterable(splits).flatMap(split -> createFlow(job, inputFormat, split));
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
    public static <T> Flowable<T> createFlow(
            Job job,
            InputFormat<?, T> inputFormat,
            InputSplit inputSplit) {
        return Flowable.generate(() -> {
                    // setup
                    RecordReader<?, T> reader = inputFormat.createRecordReader(inputSplit, new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID()));
                    // initialize
                    reader.initialize(inputSplit, new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID()));
                    return reader;
                },
                (reader, emitter) -> {
                    try {
                        if (reader.nextKeyValue()) {
                            T record = reader.getCurrentValue();
                            emitter.onNext(record);
                        } else {
                            emitter.onComplete();
                        }
                    } catch (Exception e) {
                        emitter.onError(e);
                    }
                },
                AutoCloseable::close);
    }

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