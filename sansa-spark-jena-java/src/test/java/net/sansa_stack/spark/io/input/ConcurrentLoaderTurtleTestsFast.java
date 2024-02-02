package net.sansa_stack.spark.io.input;

import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.MoreExecutors;
import net.sansa_stack.hadoop.format.jena.trig.RecordReaderRdfTrigDataset;
import net.sansa_stack.hadoop.util.FileSplitUtils;
import net.sansa_stack.spark.io.rdf.loader.AsyncRdfParserHadoop;
import org.aksw.jenax.arq.util.streamrdf.StreamRDFToUpdateRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

@RunWith(Parameterized.class)
public class ConcurrentLoaderTurtleTestsFast
//    extends RecordReaderRdfTestBase<Triple>
{
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentLoaderTurtleTestsFast.class);

    /**
     * Test case parameters
     */
    @Parameterized.Parameters(name = "{index}: file {0} with {1} splits")
    public static Iterable<Object[]> data() {
        // The map of test cases:
        // Each file is mapped to the number of  min splits and max splits(both inclusive)
        Map<String, Range<Integer>> map = new LinkedHashMap<>();

        map.put("../sansa-rdf/sansa-rdf-common/src/test/resources/nato-phonetic-alphabet-example.ttl",
                Range.closed(1, 10));

        map.put("../sansa-query/sansa-query-tests/src/main/resources/sparql11/data-r2/basic/manifest.ttl",
                Range.closed(1, 5));

        return FileSplitUtils.createTestParameters(map);
    }

    protected String file;
    protected int numSplits;

    public ConcurrentLoaderTurtleTestsFast(String file, int numSplits) {
        this.file = file;
        this.numSplits = numSplits;
    }

    public static Configuration createDefaultHadoopConfiguration() {
        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");
        conf.set(RecordReaderRdfTrigDataset.RECORD_MAXLENGTH_KEY, "10000");
        conf.set(RecordReaderRdfTrigDataset.RECORD_PROBECOUNT_KEY, "1");

        return conf;
    }

    @Test
    public void test() throws Exception {
        Dataset ds = DatasetFactory.create();

        Configuration conf = createDefaultHadoopConfiguration();

        Path effFile = new Path(java.nio.file.Paths.get(file).toAbsolutePath().toString());
        // FileSystem fileSystem = effFile.getFileSystem(conf);

        // LinkDatasetGraph linkDatasetGraph = LinkDatasetGraphSansa.create(fileSystem, () -> RDFLinkAdapter.adapt(RDFConnection.connect(ds)));

        try (RDFConnection conn = RDFConnection.connect(ds)) {
            StreamRDF sink = StreamRDFToUpdateRequest.createWithTrie(100, MoreExecutors.newDirectExecutorService(), conn::update);

            AsyncRdfParserHadoop.parse(effFile, RDFFormat.TURTLE_BLOCKS, conf, sink);
        }

        logger.debug("Dataset size: " + Iterators.size(ds.asDatasetGraph().find()));
        // RDFDataMgr.write(System.out, ds, RDFFormat.TRIG_PRETTY);
    }

    /*
    public ConcurrentLoaderTurtleTestsFast(String file, int numSplits) {
        super(file, numSplits);
    }

    @Override
    public void configureHadoop(Configuration conf) {
        super.configureHadoop(conf);
        conf.set(RecordReaderRdfTurtleTriple.RECORD_PROBECOUNT_KEY, "10");
    }

    @Override
    public InputFormat<?, Triple> createInputFormat() {
        return new FileInputFormatRdfTurtleTriple();
    }

    @Override
    public void accumulate(Dataset target, Triple contrib) {
        target.getDefaultModel().getGraph().add(contrib);
    }
    */
}