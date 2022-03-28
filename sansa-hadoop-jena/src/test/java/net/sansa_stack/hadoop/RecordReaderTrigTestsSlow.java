package net.sansa_stack.hadoop;

import com.google.common.collect.Range;
import net.sansa_stack.hadoop.format.jena.trig.FileInputFormatRdfTrigDataset;
import net.sansa_stack.hadoop.util.FileSplitUtils;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.aksw.jenax.arq.util.quad.DatasetGraphUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.jena.query.Dataset;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.LinkedHashMap;
import java.util.Map;

// @Ignore
@RunWith(Parameterized.class)
// TODO Tag with @Category()
public class RecordReaderTrigTestsSlow
    extends RecordReaderRdfTestBase<DatasetOneNg>
{
    /**
     * Test case parameters
     */
    @Parameterized.Parameters(name = "{index}: file {0} with {1} splits")
    public static Iterable<Object[]> data() {
        Map<String, Range<Integer>> map = new LinkedHashMap<>();

        // Slow test
        map.put("../sansa-resource-testdata/src/main/resources/hobbit-sensor-stream-150k-events-data.trig.bz2",
                Range.closed(1, 5));

        return FileSplitUtils.createTestParameters(map);
    }


    public RecordReaderTrigTestsSlow(String file, int numSplits) {
        super(file, numSplits);
    }

    @Override
    protected void assertIsIsomorphic(Dataset expected, Dataset actual) {
        DatasetCompareUtils.assertIsIsomorphicByGraph(expected, actual);
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
