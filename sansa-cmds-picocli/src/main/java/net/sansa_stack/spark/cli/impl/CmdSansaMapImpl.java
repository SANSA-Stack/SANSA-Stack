package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.cmd.CmdSansaMap;
import net.sansa_stack.spark.cli.util.SansaCmdUtils;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceCollection;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import net.sansa_stack.spark.io.rdf.input.api.RdfSources;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactories;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactoryImpl;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriter;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfTriplesOps;
import org.aksw.jenax.arq.util.node.NodeUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


/**
 * Called from the Java class {@link CmdSansaMap}
 */
public class CmdSansaMapImpl {
  private static Logger logger = LoggerFactory.getLogger(CmdSansaMapImpl.class);

  public static int run(CmdSansaMap cmd) throws IOException {

    List<String> inputStrs = cmd.inputFiles;

    SparkSession sparkSession = SansaCmdUtils.newDefaultSparkSessionBuilder()
            .appName("Sansa Map (" + cmd.inputFiles + ")")
            .getOrCreate();

    Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();

    RddRdfWriterFactory rddRdfWriterFactory = SansaCmdUtils.configureRdfWriter(cmd.outputConfig);
    rddRdfWriterFactory.validate();
    rddRdfWriterFactory.getPostProcessingSettings().copyFrom(cmd.postProcessConfig);


    SansaCmdUtils.validatePaths(inputStrs, hadoopConf);

    RdfSourceFactory rdfSourceFactory = RdfSourceFactories.of(sparkSession);

    RdfSourceCollection rdfSources = SansaCmdUtils.createRdfSourceCollection(rdfSourceFactory, cmd.inputFiles, cmd.inputConfig);

    RdfSource rdfSource = rdfSources;
    CmdSansaMap.MapOperation mapOp = cmd.mapOperation;
    if (mapOp != null) {
      if (Boolean.TRUE.equals(mapOp.defaultGraph)) {
        rdfSource = RdfSources.ofTriples(rdfSource.asTriples().toJavaRDD());
      } else if (mapOp.graphName != null) {
        Node graphNode = NodeUtils.createGraphNode(mapOp.graphName);
        RDD<Quad> rdd = JavaRddOfTriplesOps.mapIntoGraph(graphNode).apply(rdfSource.asTriples().toJavaRDD()).rdd();
        rdfSource = RdfSources.ofQuads(rdd.toJavaRDD());
      }
    }

    writeOutRdfSources(rdfSource, rddRdfWriterFactory);

    return 0; // exit code
  }


  public static void writeOutRdfSources(RdfSource rdfSources, RddRdfWriterFactory rddRdfWriterFactory) {

    StopWatch stopwatch = StopWatch.createStarted();

    int initialPrefixCount = rddRdfWriterFactory.getGlobalPrefixMapping().numPrefixes();

    PrefixMap declaredPrefixes = rdfSources.peekDeclaredPrefixes();
    int declaredPrefixCount = declaredPrefixes.size();

    RddRdfWriter<?> rddRdfWriter;

    if (rdfSources.usesQuads()) {
        JavaRDD<Quad> rdd = rdfSources.asQuads().toJavaRDD();
        rddRdfWriter = rddRdfWriterFactory.forQuad(rdd);
    } else {
        JavaRDD<Triple> rdd = rdfSources.asTriples().toJavaRDD();
        rddRdfWriter = rddRdfWriterFactory.forTriple(rdd);
    }
    rddRdfWriter.runUnchecked();

    Map<String, String> usedPm = Optional.ofNullable(rddRdfWriter.getGlobalPrefixMapping())
            .map(PrefixMapping::getNsPrefixMap).orElse(null);
    if (usedPm != null) {
        int usedPmCount = usedPm.size();
        logger.info(String.format("RDF writer was configured with %d prefixes. %d prefixes remained after processing. %d unique prefixes were derived from the sources.", initialPrefixCount, usedPmCount, declaredPrefixCount));
    }

    logger.info(String.format("Processing time: %.3f seconds", stopwatch.getTime(TimeUnit.MILLISECONDS) / 1000.0f));
  }
}

