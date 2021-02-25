package net.sansa_stack.rdf.common.io.hadoop;

import com.google.common.collect.Range;
import net.sansa_stack.rdf.common.io.hadoop.rdf.trig.RecordReaderTrigDataset;
import net.sansa_stack.rdf.common.io.hadoop.rdf.turtle.FileInputFormatTurtleTriple;
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
    public RecordReaderTurtleTestsFast(String file, int numSplits) {
        super(file, numSplits);
    }

    @Override
    public void configureHadoop(Configuration conf) {
        super.configureHadoop(conf);
        conf.set(RecordReaderTrigDataset.PROBE_RECORD_COUNT_KEY, "20");
    }

    @Override
    public InputFormat<?, Triple> createInputFormat() {
        return new FileInputFormatTurtleTriple();
    }

    @Override
    public void accumulate(Dataset target, Triple contrib) {
        target.getDefaultModel().getGraph().add(contrib);
    }

    /**
     * Test case parameters
     *
     * @return
     */
    @Parameterized.Parameters(name = "{index}: file {0} with {1} splits")
    public static Iterable<Object[]> data() {
        // The map of test cases:
        // Each file is mapped to the number of  min splits and max splits(both inclusive)
        Map<String, Range<Integer>> map = new LinkedHashMap<>();

        map.put("../../sansa-rdf/sansa-rdf-common/src/test/resources/nato-phonetic-alphabet-example.ttl",
                Range.closed(1, 5));

//        map.put("../../sansa-query/sansa-query-tests/src/main/resources/sparql11/data-r2/basic/manifest.ttl",
//                Range.closed(1, 5));

        return RecordReaderRdfTestBase.createParameters(map);
    }
}