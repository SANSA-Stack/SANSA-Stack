package net.sansa_stack.spark.io.rdf.input.api;

import net.sansa_stack.hadoop.core.InputFormatStats;
import net.sansa_stack.hadoop.core.RecordReaderGenericBase;
import net.sansa_stack.hadoop.core.SansaHadoopConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class InputFormatUtils {
    /**
     * Wrap an input format that is based on {@link RecordReaderGenericBase} with an analyzer that turns
     * each split into parsing metadata rather than data.
     */
    public static HadoopInputData<LongWritable, Resource, JavaRDD<Model>> wrapWithAnalyzer(HadoopInputData<?, ?, ?> hid) {
        Class<?> inputFormatClass = hid.getInputFormatClass();
        Configuration conf = hid.getConfiguration();
        String delegateClassName = inputFormatClass.getName();
        conf.set(SansaHadoopConstants.DELEGATE, delegateClassName);
        String path = hid.getPath();
        return new HadoopInputData<>(path, InputFormatStats.class, LongWritable.class, Resource.class, conf, javaPairRdd -> javaPairRdd.map(x -> x._2.getModel()));
    }

    public static <K, V, X> X createRdd(JavaSparkContext sc, HadoopInputData<K, V, X> inputData) {
        JavaPairRDD<K, V> pairRdd = sc.newAPIHadoopFile(inputData.getPath(),
                inputData.getInputFormatClass(), inputData.getKeyClass(), inputData.getValueClass(), inputData.getConfiguration());
        X result = inputData.getMapper().apply(pairRdd);
        return result;
    }

}
