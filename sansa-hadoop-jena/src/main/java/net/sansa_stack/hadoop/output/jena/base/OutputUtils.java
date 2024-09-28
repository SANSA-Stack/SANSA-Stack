package net.sansa_stack.hadoop.output.jena.base;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;

public class OutputUtils {
    public static final String NUM_SPLITS = "mapreduce.output.split.count";

    public static Configuration setSplitCount(Configuration conf, int value) {
        conf.setInt(NUM_SPLITS, value);
        return conf;
    }

    public static int getSplitCount(Configuration conf) {
        int result = conf.getInt(NUM_SPLITS, -1);
        Preconditions.checkArgument(result >= 0, "Hadoop configuration object lacks required value for key " + NUM_SPLITS);
        return result;
    }
}
