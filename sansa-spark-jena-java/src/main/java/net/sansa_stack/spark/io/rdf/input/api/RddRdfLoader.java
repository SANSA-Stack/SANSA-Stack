package net.sansa_stack.spark.io.rdf.input.api;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.jena.rdf.model.Model;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

/** An RddRdfLoader provides rdf-related methods to operate on paths w.r.t. a sparkContext */
public interface RddRdfLoader<T> {

    Class<T> getValueClass();
    Class<? extends FileInputFormat<LongWritable, T>> getFileInputFormatClass();

    RDD<T> load(SparkContext sparkContext, String path);

    // TODO Augment with a generic openRdfStreamIterator
    /** Peek prefixes w.r.t. the hadoop configuration and the
     * loader's FileInputFormat */
    Model peekPrefixes(SparkContext sparkContext, String path);
}
