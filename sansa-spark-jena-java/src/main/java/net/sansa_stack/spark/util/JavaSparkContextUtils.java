package net.sansa_stack.spark.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class JavaSparkContextUtils {
    public static JavaSparkContext fromRdd(JavaRDD<?> rdd) {
        return JavaSparkContext.fromSparkContext(rdd.context());
    }
}
