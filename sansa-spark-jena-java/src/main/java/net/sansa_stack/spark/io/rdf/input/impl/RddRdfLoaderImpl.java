package net.sansa_stack.spark.io.rdf.input.impl;

import net.sansa_stack.hadoop.format.jena.base.CanParseRdf;
import net.sansa_stack.spark.io.rdf.input.api.RddLoaderBase;
import net.sansa_stack.spark.io.rdf.input.api.RddRdfLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

public class RddRdfLoaderImpl<T>
    extends RddLoaderBase<LongWritable, T>
	implements RddRdfLoader<T>
{
    public RddRdfLoaderImpl(Class<T> clazz, Class<? extends FileInputFormat<LongWritable, T>> fileInputFormatClass) {
		super(clazz, fileInputFormatClass);
	}

	@Override
    public RDD<T> load(SparkContext sparkContext, String path) {
        return RddRdfLoaders.createRdd(sparkContext, path, clazz, fileInputFormatClass);
    }

    @Override
    public PrefixMap peekPrefixes(SparkContext sparkContext, String path) {
        PrefixMap result;
        try {
            FileInputFormat<LongWritable, T> fif = fileInputFormatClass.getDeclaredConstructor().newInstance();
            if (fif instanceof CanParseRdf) {
                CanParseRdf feature = (CanParseRdf)fif;
                Configuration conf = sparkContext.hadoopConfiguration();
                FileSystem fs = FileSystem.get(conf);
                result = feature.parsePrefixes(fs, new Path(path), conf);
            } else {
                // Return empty prefixes - e.g. for ntriples
                result = PrefixMapFactory.create();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }
}
