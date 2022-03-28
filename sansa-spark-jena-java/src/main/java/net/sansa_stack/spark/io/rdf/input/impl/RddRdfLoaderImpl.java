package net.sansa_stack.spark.io.rdf.input.impl;

import net.sansa_stack.hadoop.format.jena.base.CanParseRdf;
import net.sansa_stack.spark.io.rdf.input.api.RddRdfLoader;
import net.sansa_stack.spark.io.rdf.input.api.RddRdfLoaderRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.jena.hadoop.rdf.io.input.nquads.NQuadsInputFormat;
import org.apache.jena.hadoop.rdf.types.QuadWritable;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.InvocationTargetException;

public class RddRdfLoaderImpl<T>
    implements RddRdfLoader<T>
{
    protected Class<T> clazz;
    protected Class<? extends FileInputFormat<LongWritable, T>> fileInputFormatClass;

    public RddRdfLoaderImpl(Class<T> clazz, Class<? extends FileInputFormat<LongWritable, T>> fileInputFormatClass) {
        this.clazz = clazz;
        this.fileInputFormatClass = fileInputFormatClass;
    }

    @Override
    public RDD<T> load(SparkContext sparkContext, String path) {
        return RddRdfLoaders.createRdd(sparkContext, path, clazz, fileInputFormatClass);
    }

    @Override
    public Model peekPrefixes(SparkContext sparkContext, String path) {
        Model result;
        try {
            FileInputFormat<LongWritable, T> fif = fileInputFormatClass.getDeclaredConstructor().newInstance();
            if (fif instanceof CanParseRdf) {
                CanParseRdf feature = (CanParseRdf)fif;
                Configuration conf = sparkContext.hadoopConfiguration();
                FileSystem fs = FileSystem.get(conf);
                result = feature.parsePrefixes(fs, new Path(path), conf);
            } else {
                // Return empty prefixes - e.g. for ntriples
                result = ModelFactory.createDefaultModel();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }
}
