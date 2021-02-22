package net.sansa_stack.rdf.common.io.hadoop;

import com.google.common.collect.*;
import org.aksw.jena_sparql_api.utils.DatasetGraphUtils;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test cases for the {@link TrigRecordReader}:
 * A given set of test datasets (represented as trig files) is first split into a configurable
 * number of splits from which the overall graph is then reassembled.
 * The reassembled dataset must match the one of the original file.
 *
 * @author Lorenz Buehmann
 * @author Claus Stadler
 */
@RunWith(Parameterized.class)
public class TrigRecordReaderTest {

    private static final Logger logger = LoggerFactory.getLogger(TrigRecordReaderTest.class);

    /**
     * Test case parameters
     *
     * @return
     */
    @Parameterized.Parameters(name = "{index}: file {0} with {1} splits")
    public static Iterable<Object[]> data() {
        // The map of test cases:
        // Each file is mapped to the number of  min splits and max splits(both inclusive)
        Map<String, Range<Integer>> params = new LinkedHashMap<>();

        params.put("src/test/resources/nato-phonetic-alphabet-example.trig",
                Range.closed(1, 5));

        params.put("src/test/resources/nato-phonetic-alphabet-example.trig.bz2",
                Range.closed(1, 5));

        // Slow test
        // params.put("../../sansa-resource-testdata/src/main/resources/hobbit-sensor-stream-150k-events-data.trig.bz2",
        //        Range.closed(1, 5));

        // Post process the map into junit params by enumerating the ranges
        // and creating a test case for each obtained value
        List<Object[]> result = params.entrySet().stream()
                .flatMap(e -> ContiguousSet.create(e.getValue(), DiscreteDomain.integers()).stream()
                        .map(numSplits -> new Object[]{e.getKey(), numSplits}))
                .collect(Collectors.toList());

        return result;
    }

    protected String file;
    protected int numSplits;

    public TrigRecordReaderTest(String file, int numSplits) {
        this.file = file;
        this.numSplits = numSplits;
    }

    @Test
    public void test() throws IOException, InterruptedException {

        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");
        conf.set(TrigRecordReader.MAX_RECORD_LENGTH, "10000");
        conf.set(TrigRecordReader.PROBE_RECORD_COUNT, "1");

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
        Dataset expectedDataset = RDFDataMgr.loadDataset(referencePath.toString());

        long fileLengthTotal = Files.size(testPath);


        Job job = Job.getInstance(conf);
        TrigFileInputFormat inputFormat = new TrigFileInputFormat();

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
        Dataset actualDataset = testSplit(job, inputFormat, testHadoopPath, fileLengthTotal, numSplits);
        logger.info("Dataset contains " + Iterators.size(actualDataset.listNames()) + " named graphs"); // - total contribs = " + totalContrib);

        compareDatasets(expectedDataset, actualDataset);
    }

    //test("parsing Trig file provided by $i splits") {

    public Dataset testSplit(
            Job job,
            TrigFileInputFormat inputFormat,
            org.apache.hadoop.fs.Path testHadoopPath,
            long fileTotalLength,
            int numSplits) throws IOException, InterruptedException {
        List<InputSplit> splits = generateFileSplits(testHadoopPath, fileTotalLength, numSplits);

        Dataset ds = DatasetFactory.create();
        int totalContrib = 0;
        for (InputSplit split : splits) {
            // println(s"split (${split.getStart} - ${split.getStart + split.getLength}):" )

          /*
                  val stream = split.getPath.getFileSystem(new TaskAttemptContextImpl(conf, new TaskAttemptID()).getConfiguration)
                    .open(split.getPath)

                  val bufferSize = split.getLength.toInt
                  val buffer = new Array[Byte](bufferSize)
                  stream.readFully(split.getStart, buffer, 0, bufferSize)
                  println(new String(buffer))
                  stream.close()
                  */

            // setup
            RecordReader<LongWritable, Dataset> reader = inputFormat.createRecordReader(split, new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID()));
            //        val reader = new TrigRecordReader()

            // initialize
            reader.initialize(split, new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID()));

            // read all records in split
            Dataset contrib = consumeRecords(reader);
            totalContrib += Iterators.size(contrib.listNames());
            DatasetGraphUtils.addAll(ds.asDatasetGraph(), contrib.asDatasetGraph());
            //        System.err.println("Dataset contribution")
            //        RDFDataMgr.write(System.err, ds, RDFFormat.TRIG_PRETTY)
        }

        return ds;
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
    public Dataset consumeRecords(RecordReader<LongWritable, Dataset> reader) throws IOException, InterruptedException {
        Dataset result = DatasetFactory.create();
        // val actual = new mutable.ListBuffer[(LongWritable, Dataset)]()
        // var counter = 0;
        while (reader.nextKeyValue()) {
            LongWritable k = reader.getCurrentKey();
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

    public List<InputSplit> generateFileSplits(org.apache.hadoop.fs.Path testHadoopPath, long fileLengthTotal, int numSplits) throws IOException {
        int splitLength = (int) (fileLengthTotal / (double) numSplits);

        List<InputSplit> result = IntStream.range(0, numSplits)
                .mapToObj(i -> {
                    long start = i * splitLength;
                    long end = Math.min((i + 1) * splitLength, fileLengthTotal);
                    long length = end - start;

                    return (InputSplit) new FileSplit(testHadoopPath, start, length, null);
                })
                .collect(Collectors.toList());
        return result;
    }


    public static void compareDatasets(Dataset ds1, Dataset ds2) {

    /*

    val a = Sets.newHashSet(ds1.asDatasetGraph().find())
    val b = Sets.newHashSet(ds2.asDatasetGraph().find())

    val diff = new SetDiff[Quad](a, b)

    System.err.println("Excessive")
    for(x <- diff.getAdded.asScala) {
      System.err.println("  " + x)
    }

    System.err.println("Missing")
    for(x <- diff.getRemoved.asScala) {
      System.err.println("  " + x)
    }

    System.err.println("Report done")
    */

    /*
    System.err.println("Dataset 1")
    RDFDataMgr.write(System.err, ds1, RDFFormat.TRIG_PRETTY)
    System.err.println("Dataset 2")
    RDFDataMgr.write(System.err, ds2, RDFFormat.TRIG_PRETTY)
    System.err.println("Datasets printed")
    */

        // compare default graphs first
        Assert.assertTrue("default graphs do not match", ds1.getDefaultModel().getGraph().isIsomorphicWith(ds2.getDefaultModel().getGraph()));

        // then compare the named graphs
        Set<String> allNames = Sets.union(Sets.newHashSet(ds1.listNames()), Sets.newHashSet(ds2.listNames()));

        for (String g : allNames) {

            Assert.assertTrue("graph <" + g + "> not found in first dataset", ds1.containsNamedModel(g));
            Assert.assertTrue("graph <" + g + "> not found in second dataset", ds2.containsNamedModel(g));

            Graph g1 = ds1.getNamedModel(g).getGraph();
            Graph g2 = ds2.getNamedModel(g).getGraph();

            Assert.assertEquals("size of graph <" + g + "> not the same in both datasets", g1.size(), g2.size());
            // Isomorphism check may fail with stack overflow execution if datasets
            // become too large
            // assert(g1.isIsomorphicWith(g2), s"graph <$g> not isomorph")
        }
    }
}