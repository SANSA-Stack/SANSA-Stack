package net.sansa_stack.spark.util;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SansaSessionUtils {
    private static final Logger logger = LoggerFactory.getLogger(SansaSessionUtils.class);

    public static final String KRYO_BUFFER_MAX_KEY = "spark.kryo.serializer.buffer.max";

    public static SparkSession.Builder newDefaultSparkSessionBuilder() {
        SparkSession.Builder result = SparkSession.builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", String.join(", ",
                        "net.sansa_stack.spark.io.rdf.kryo.JenaKryoRegistrator"));

        String value; // for debugging / inspection
        if ((value = System.getProperty("spark.master")) == null) {
            String defaultMaster = "local[*]";
            logger.info("'spark.master' not set - defaulting to: " + defaultMaster);
            result = result.master(defaultMaster);
        }

        if ((value = System.getProperty(KRYO_BUFFER_MAX_KEY)) == null) {
            result = result.config(KRYO_BUFFER_MAX_KEY, "2048"); // MB
        }
        return result;
    }
}
