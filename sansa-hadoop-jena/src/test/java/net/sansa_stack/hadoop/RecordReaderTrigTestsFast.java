package net.sansa_stack.hadoop;

import com.google.common.collect.Range;
import net.sansa_stack.hadoop.format.jena.trig.FileInputFormatRdfTrigDataset;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.aksw.jenax.arq.util.quad.DatasetGraphUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.jena.query.Dataset;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.LinkedHashMap;
import java.util.Map;

@RunWith(Parameterized.class)
public class RecordReaderTrigTestsFast
    extends RecordReaderRdfTestBase<DatasetOneNg>
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
    public InputFormat<?, DatasetOneNg> createInputFormat() {
        return new FileInputFormatRdfTrigDataset();
    }

    @Override
    public void accumulate(Dataset target, DatasetOneNg contrib) {
        DatasetGraphUtils.addAll(target.asDatasetGraph(), contrib.asDatasetGraph());
    }
}