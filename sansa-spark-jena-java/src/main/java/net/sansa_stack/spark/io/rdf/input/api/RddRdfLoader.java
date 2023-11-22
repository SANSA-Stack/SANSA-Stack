package net.sansa_stack.spark.io.rdf.input.api;

import org.apache.hadoop.io.LongWritable;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.spark.SparkContext;

/** An RddRdfLoader provides rdf-related methods to operate on paths w.r.t. a sparkContext */
public interface RddRdfLoader<T>
	extends RddLoader<LongWritable, T>
{
    // TODO Augment with a generic openRdfStreamIterator
    /** Peek prefixes w.r.t. the hadoop configuration and the
     * loader's FileInputFormat */
    PrefixMap peekPrefixes(SparkContext sparkContext, String path);
}
