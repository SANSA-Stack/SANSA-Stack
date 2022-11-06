package net.sansa_stack.hadoop;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.aksw.jenax.arq.dataset.orderaware.DatasetFactoryEx;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sansa_stack.hadoop.core.InputFormatStats;
import net.sansa_stack.hadoop.core.Stats2;

public abstract class RecordReaderStatsBase {

    private static final Logger logger = LoggerFactory.getLogger(RecordReaderRdfTestBase.class);

    protected String file;
    protected int numSplits;

    public RecordReaderStatsBase(String file, int numSplits) {
        this.file = file;
        this.numSplits = numSplits;
    }

    protected abstract InputFormat<?, ?> createInputFormat();
    // protected abstract void accumulate(Dataset target, ? contrib);

    /** Override this for custom hadoop configuration */
    protected void configureHadoop(Configuration conf) {}

    /**
     * Override this to use a different Dataset implementation
     * such as {@link DatasetFactoryEx#createInsertOrderPreservingDataset()}
     */
    protected Dataset createDataset() {
        return DatasetFactory.create();
    }


    @Test
    public void test() throws IOException, InterruptedException {
        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");
        // conf.set(RecordReaderRdfTrigDataset.RECORD_MAXLENGTH_KEY, "10000");
        // conf.set(RecordReaderRdfTrigDataset.RECORD_PROBECOUNT_KEY, "2");

        configureHadoop(conf);

        Path referencePath = Paths.get(file).toAbsolutePath();
        Path testPath = referencePath;

        long fileLengthTotal = Files.size(testPath);

        Job job = Job.getInstance(conf);
        // TrigFileInputFormat inputFormat = new TrigFileInputFormat();
        InputFormat<?, ?> baseInputFormat = createInputFormat();
        InputFormat<?, Resource> inputFormat = new InputFormatStats(baseInputFormat);

        // add input path of the file
        org.apache.hadoop.fs.Path testHadoopPath = new org.apache.hadoop.fs.Path(testPath.toString());
        FileInputFormat.addInputPath(job, testHadoopPath);

        // call once to compute the prefixes
        // inputFormat.getSplits(job);
        try (Stream<Stats2> stats = RecordReaderRdfTestBase.testSplit(job, inputFormat, testHadoopPath, fileLengthTotal, numSplits).map(r -> r.as(Stats2.class))) {
            stats.forEach(x -> RDFDataMgr.write(System.out, x.getModel(), RDFFormat.TURTLE_PRETTY));
        }
    }

}
