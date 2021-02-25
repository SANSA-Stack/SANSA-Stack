package net.sansa_stack.hadoop;

import com.google.common.collect.Range;
import net.sansa_stack.hadoop.jena.rdf.trig.FileInputFormatTrigDataset;
import org.aksw.jena_sparql_api.utils.DatasetGraphUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.jena.query.Dataset;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.LinkedHashMap;
import java.util.Map;

@RunWith(Parameterized.class)
public class RecordReaderTrigTestsFast
    extends RecordReaderRdfTestBase<Dataset>
{
    /**
     * Test case parameters
     */
    @Parameterized.Parameters(name = "{index}: file {0} with {1} splits")
    public static Iterable<Object[]> data() {
        // The map of test cases:
        // Each file is mapped to the number of  min splits and max splits(both inclusive)
        Map<String, Range<Integer>> map = new LinkedHashMap<>();

        map.put("../sansa-rdf/sansa-rdf-common/src/test/resources/nato-phonetic-alphabet-example.trig",
                Range.closed(1, 5));

        map.put("../sansa-rdf/sansa-rdf-common/src/test/resources/nato-phonetic-alphabet-example.trig.bz2",
                Range.closed(1, 5));

        return createParameters(map);
    }

    public RecordReaderTrigTestsFast(String file, int numSplits) {
        super(file, numSplits);
    }

    @Override
    public InputFormat<?, Dataset> createInputFormat() {
        return new FileInputFormatTrigDataset();
    }

    @Override
    public void accumulate(Dataset target, Dataset contrib) {
        DatasetGraphUtils.addAll(target.asDatasetGraph(), contrib.asDatasetGraph());
    }
}