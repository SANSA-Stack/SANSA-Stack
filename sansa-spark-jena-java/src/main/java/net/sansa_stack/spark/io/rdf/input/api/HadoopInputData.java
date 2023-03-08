package net.sansa_stack.spark.io.rdf.input.api;

import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * A class to capture the arguments of {@link JavaSparkContext#newAPIHadoopFile(String, Class, Class, Class, Configuration)}.
 * Furthermore, captures a mapping of the initial JavaPairRDD to a custom target type (typically another JavaRDD).
 *
 * To create RDDs from objects of this class use {@link InputFormatUtils#createRdd(JavaSparkContext, HadoopInputData)}.
 * HadoopInputData based on the {@link net.sansa_stack.hadoop.core.RecordReaderGenericBase} can be wrapped using
 * {@link InputFormatUtils#wrapWithAnalyzer(HadoopInputData)} which returns another HadoopInputData object.
 * RDDs created from the wrapper compute information
 * about the detected records, such as the byte offsets and the time it took to parse them.
 */
public class HadoopInputData<K, V, X> {
    protected String path;
    protected Class<? extends InputFormat<K, V>> inputFormatClass;
    protected Class<K> keyClass;
    protected Class<V> valueClass;
    protected Configuration configuration;
    protected Function<JavaPairRDD<K, V>, X> mapper;

    public HadoopInputData(String path, Class<? extends InputFormat<K, V>> inputFormatClass,
            Class<K> keyClass, Class<V> valueClass, Configuration configuration,
            Function<JavaPairRDD<K, V>, X> mapper) {
        super();
        this.path = path;
        this.inputFormatClass = inputFormatClass;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.configuration = configuration;
        this.mapper = mapper;
    }

    public String getPath() {
        return path;
    }

    public Class<K> getKeyClass() {
        return keyClass;
    }

    public Class<V> getValueClass() {
        return valueClass;
    }

    public Class<? extends InputFormat<K, V>> getInputFormatClass() {
        return inputFormatClass;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public Function<JavaPairRDD<K, V>, X> getMapper() {
        return mapper;
    }

    /** Return a fresh {@link HadoopInputData} instance where "nextMapper" is applied to the result of the current mapper */
    public <Y> HadoopInputData<K, V, Y> map(Function<? super X, Y> nextMapper) {
        return new HadoopInputData<>(path, inputFormatClass, keyClass, valueClass, configuration, mapper.andThen(nextMapper));
    }
}
