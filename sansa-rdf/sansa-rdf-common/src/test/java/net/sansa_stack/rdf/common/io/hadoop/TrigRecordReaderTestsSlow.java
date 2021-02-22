package net.sansa_stack.rdf.common.io.hadoop;

import com.google.common.collect.Range;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.LinkedHashMap;
import java.util.Map;

// @RunWith(Parameterized.class)
// @Category()
public class TrigRecordReaderTestsSlow
    extends TrigRecordReaderTestBase
{
    public TrigRecordReaderTestsSlow(String file, int numSplits) {
        super(file, numSplits);
    }

    /**
     * Test case parameters
     *
     * @return
     */
    @Parameterized.Parameters(name = "{index}: file {0} with {1} splits")
    public static Iterable<Object[]> data() {
        Map<String, Range<Integer>> map = new LinkedHashMap<>();

        // Slow test
        map.put("../../sansa-resource-testdata/src/main/resources/hobbit-sensor-stream-150k-events-data.trig.bz2",
                Range.closed(1, 5));

        return TrigRecordReaderTestBase.createParameters(map);
    }
}
