package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.cmd.CmdMixinSparkPostProcess;
import net.sansa_stack.spark.cli.cmd.CmdSansaMap;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceCollection;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactoryImpl;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriter;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfQuadsOps;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfTriplesOps;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOps;
import org.aksw.jenax.arq.analytics.NodeAnalytics;
import org.aksw.jenax.arq.util.quad.QuadUtils;
import org.aksw.jenax.arq.util.triple.TripleUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * Called from the Java class {@link CmdSansaMap}
 */
public class CmdSansaMapImpl {
  private static Logger logger = LoggerFactory.getLogger(CmdSansaMapImpl.class);

  public static int run(CmdSansaMap cmd) throws IOException {

    List<String> inputStrs = cmd.inputFiles;

    SparkSession sparkSession = CmdUtils.newDefaultSparkSessionBuilder()
            .appName("Sansa Sort (" + cmd.inputFiles + ")")
            .getOrCreate();

    Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();

    RddRdfWriterFactory rddRdfWriterFactory = CmdUtils.configureWriter(cmd.outputConfig);
    rddRdfWriterFactory.getPostProcessingSettings().copyFrom(cmd.postProcessConfig);


    CmdUtils.validatePaths(inputStrs, hadoopConf);

    RdfSourceFactory rdfSourceFactory = RdfSourceFactoryImpl.from(sparkSession);

    RdfSourceCollection rdfSources = CmdUtils.createRdfSourceCollection(rdfSourceFactory, cmd.inputFiles, cmd.inputConfig);

    writeOutRdfSources(rdfSources, rddRdfWriterFactory);

    return 0; // exit code
  }


  public static void writeOutRdfSources(RdfSource rdfSources, RddRdfWriterFactory rddRdfWriterFactory) {

    StopWatch stopwatch = StopWatch.createStarted();

    int initialPrefixCount = rddRdfWriterFactory.getGlobalPrefixMapping().numPrefixes();

    Model declaredPrefixes = rdfSources.peekDeclaredPrefixes();
    int declaredPrefixCount = declaredPrefixes.numPrefixes();

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
      logger.info(String.format("Optimization of prefixes reduced their number from %d to %d (of which %d originated from the sources)", initialPrefixCount, usedPmCount, declaredPrefixCount));
    }

    logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");

  }
}

