package net.sansa_stack.spark.io.rdf.input.api;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

public interface RddLoader<K, T> {
    Class<T> getValueClass();
    Class<? extends FileInputFormat<K, T>> getFileInputFormatClass();

    RDD<T> load(SparkContext sparkContext, String path);
}
