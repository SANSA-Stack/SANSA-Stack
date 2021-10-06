package net.sansa_stack.hadoop;

import com.google.common.collect.Range;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.LinkedHashMap;
import java.util.Map;

@RunWith(Parameterized.class)
public class RecordReaderCsvTestsFast
    extends RecordReaderCsvTestBase
{
    /**
     * Test case parameters
     */
    @Parameterized.Parameters(name = "{index}: file {0} with {1} splits")
    public static Iterable<Object[]> data() {
        // The map of test cases:
        // Each file is mapped to the number of  min splits and max splits(both inclusive)
        Map<String, Range<Integer>> map = new LinkedHashMap<>();

//        map.put("/home/raven/Datasets/bio2rdf_sparql_logs_01-2019_to_07-2021.head.csv",
//                Range.closed(1, 10));

//           map.put("/home/raven/Datasets/bio2rdf_sparql_logs_processed_01-2019_to_07-2021.head10000.csv",
//                Range.closed(1, 10));


//        map.put("src/test/resources/test-data.json.bz2",
//                Range.closed(1, 5));

        return createParameters(map);
    }

    public RecordReaderCsvTestsFast(String file, int numSplits) {
        super(file, numSplits);
    }
}