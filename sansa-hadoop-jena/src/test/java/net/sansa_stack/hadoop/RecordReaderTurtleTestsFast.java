package net.sansa_stack.hadoop;

import com.google.common.collect.Range;
import net.sansa_stack.hadoop.format.jena.turtle.FileInputFormatRdfTurtleTriple;
import net.sansa_stack.hadoop.format.jena.turtle.RecordReaderRdfTurtleTriple;
import net.sansa_stack.hadoop.util.FileSplitUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.LinkedHashMap;
import java.util.Map;

@RunWith(Parameterized.class)
public class RecordReaderTurtleTestsFast
    extends RecordReaderRdfTestBase<Triple>
{
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


    public RecordReaderTurtleTestsFast(String file, int numSplits) {
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
}