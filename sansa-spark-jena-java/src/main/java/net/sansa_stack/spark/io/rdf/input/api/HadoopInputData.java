package net.sansa_stack.spark.io.rdf.input.api;

import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.api.java.JavaPairRDD;

/** A class to capture the arguments of sparkContext.newApiHadoopRDD */
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
}
