package net.sansa_stack.hadoop;

import com.google.common.collect.Range;
import net.sansa_stack.hadoop.format.jena.trig.FileInputFormatRdfTrigDataset;
import net.sansa_stack.hadoop.format.jena.trig.RecordReaderRdfTrigDataset;
import net.sansa_stack.hadoop.util.FileSplitUtils;
import org.aksw.commons.io.util.FileUtils;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.aksw.jenax.arq.util.quad.DatasetGraphUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sys.JenaSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

@RunWith(Parameterized.class)
// @Ignore
public class RecordReaderTrigTestsSkewed
        extends RecordReaderRdfTestBase<DatasetOneNg>
{
    static {
        JenaSystem.init();
    }

    protected static Path dataPath = Path.of("/tmp/data2.trig");

    @BeforeClass
    public static void tearUp() throws Exception {
        Files.deleteIfExists(dataPath);
        generateData2(dataPath, new int[] {100000});
    }

    @AfterClass
    public static void tearDown() throws Exception {
        // Files.deleteIfExists(dataPath);
    }

    /**
     * Test case parameters
     */
    @Parameterized.Parameters(name = "{index}: file {0} with {1} splits")
    public static Iterable<Object[]> data() throws Exception {
        // The map of test cases:
        // Each file is mapped to the number of  min splits and max splits(both inclusive)
        Map<String, Range<Integer>> map = new LinkedHashMap<>();

        map.put(dataPath.toString(), Range.closed(1, 10));

//        map.put("../sansa-rdf/sansa-rdf-common/src/test/resources/nato-phonetic-alphabet-example.trig.bz2",
//                Range.closed(1, 5));

        return FileSplitUtils.createTestParameters(map);
    }

    public RecordReaderTrigTestsSkewed(String file, int numSplits) {
        super(file, numSplits);
    }

    @Override
    public InputFormat<?, DatasetOneNg> createInputFormat() {
        return new FileInputFormatRdfTrigDataset();
    }

    @Override
    public void accumulate(Dataset target, DatasetOneNg contrib) {
        DatasetGraphUtils.addAll(target.asDatasetGraph(), contrib.asDatasetGraph());
    }


    public static void generateData1(Path path, int[] graphSizes) throws Exception {
        FileUtils.safeCreate(path,
                out -> OutputStreamRightPad.rightPad(out, 100),
                FileUtils.OverwriteMode.SKIP, out -> {
            StreamRDF sink = StreamRDFWriter.getWriterStream(out, RDFFormat.TRIG_BLOCKS);
            sink.start();
            // int[] graphSizes = {1000000};

            for (int i = 0; i < graphSizes.length; ++i) {
                int graphSize = graphSizes[i];
                for (int j = 0; j < graphSize; ++j) {
                    String prefix = "urn:x-foo:";
                    Node g = NodeFactory.createURI(prefix + "g" + j);
                    Node s = NodeFactory.createURI(prefix + "s" + j);
                    Node p = NodeFactory.createURI(prefix + "p" + j);
                    Node o = NodeFactory.createURI(prefix + "o" + j);
                    Quad quad = Quad.create(g, s, p, o);
                    sink.quad(quad);
                }
            }
            sink.finish();
        });
    }

    public static void generateData2(Path path, int[] graphSizes) throws Exception {
        FileUtils.safeCreate(path,
                out -> OutputStreamRightPad.rightPad(out, 100),
                FileUtils.OverwriteMode.SKIP,
                out -> {
            StreamRDF sink = StreamRDFWriter.getWriterStream(out, RDFFormat.TRIG_BLOCKS);
            sink.start();

            for (int i = 0; i < graphSizes.length; ++i) {
                int graphSize = graphSizes[i];
                String prefix = "http://example.org/g" + i;
                Node g = NodeFactory.createURI(prefix);
                for (int j = 0; j < graphSize; ++j) {
                    Node s = NodeFactory.createURI(prefix + "/s" + j);
                    Node p = NodeFactory.createURI(prefix + "/p" + j);
                    Node o = NodeFactory.createURI(prefix + "/o" + j);
                    Quad quad = Quad.create(g, s, p, o);
                    sink.quad(quad);
                }
            }
            sink.finish();
        });
    }

    @Override
    public void configureHadoop(Configuration conf) {
        super.configureHadoop(conf);
        conf.set(RecordReaderRdfTrigDataset.RECORD_MAXLENGTH_KEY, "1000000000");
        conf.set(RecordReaderRdfTrigDataset.RECORD_PROBECOUNT_KEY, "1000000000");
    }
}
