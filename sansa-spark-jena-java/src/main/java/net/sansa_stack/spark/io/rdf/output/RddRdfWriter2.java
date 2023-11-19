package net.sansa_stack.spark.io.rdf.output;

import net.sansa_stack.hadoop.output.jena.base.OutputFormatStreamRdfQuad;
import net.sansa_stack.hadoop.output.jena.base.OutputFormatStreamRdfTriple;
import net.sansa_stack.hadoop.output.jena.base.OutputUtils;
import net.sansa_stack.hadoop.output.jena.base.RdfOutputUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

/**
 * Core class for configuration and execution of writing RDDs of RDF out using Hadaop.
 * An RDD's number splits into the hadoop conf and the OutputFormat uses it to decide
 * whether to output header / footer data blocks on the first / last partitions.
 */
public class RddRdfWriter2
{
    protected RDFFormat rdfFormat;
    protected boolean mapQuadsToTriplesForTripleLangs;
    protected PrefixMapping prefixes;

    public RddRdfWriter2(RDFFormat rdfFormat, boolean mapQuadsToTriplesForTripleLangs, PrefixMapping prefixes) {
        super();
        this.rdfFormat = rdfFormat;
        this.mapQuadsToTriplesForTripleLangs = mapQuadsToTriplesForTripleLangs;
        this.prefixes = prefixes;
    }

    public void writeTriples(RDD<Triple> rdd, Path path) {
        Configuration conf = buildConfiguration(rdd);
        JavaPairRDD<Long, Triple> pairRdd = toPairRdd(rdd.toJavaRDD());
        pairRdd.saveAsNewAPIHadoopFile(path.toString(),
                Long.class,
                Triple.class,
                OutputFormatStreamRdfTriple.class,
                conf);
    }

    public void writeQuads(RDD<Quad> rdd, Path path) {
        Configuration conf = buildConfiguration(rdd);
        JavaPairRDD<Long, Quad> pairRdd = toPairRdd(rdd.toJavaRDD());
        pairRdd.saveAsNewAPIHadoopFile(path.toString(),
                Long.class,
                Quad.class,
                OutputFormatStreamRdfQuad.class,
                conf);
    }

    public static Configuration buildBaseConfiguration(RDD<?> rdd) {
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(rdd.context());
        Configuration baseConf = sparkContext.hadoopConfiguration();
        Configuration result = new Configuration(baseConf);

        int numPartitions =  rdd.getNumPartitions();
        OutputUtils.setSplitCount(result, numPartitions);
        return result;
    }

    protected Configuration buildConfiguration(RDD<?> rdd) {
        Configuration result = buildBaseConfiguration(rdd);
        configure(result);
        return result;
    }

    protected void configure(Configuration conf) {
        RdfOutputUtils.setRdfFormat(conf, rdfFormat);
        RdfOutputUtils.setPrefixes(conf, prefixes);
        RdfOutputUtils.setMapQuadsToTriplesForTripleLangs(conf, mapQuadsToTriplesForTripleLangs);
    }

    public static <T> JavaPairRDD<Long, T> toPairRdd(JavaRDD<T> rdd) {
        JavaPairRDD<Long, T> result = rdd
                .mapToPair(v -> new Tuple2<>(0l, v));
        return result;
    }
}
