package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.cmd.CmdMixinSparkPostProcess;
import net.sansa_stack.spark.cli.cmd.CmdSansaMap;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceCollection;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactoryImpl;
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
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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
    int initialPrefixes = rddRdfWriterFactory.getGlobalPrefixMapping().numPrefixes();

    CmdUtils.validatePaths(inputStrs, hadoopConf);

    RdfSourceFactory rdfSourceFactory = RdfSourceFactoryImpl.from(sparkSession);

    RdfSourceCollection rdfSources = CmdUtils.createRdfSourceCollection(rdfSourceFactory, cmd.inputFiles, cmd.inputConfig);

    CmdMixinSparkPostProcess ppc = cmd.postProcessConfig;


    StopWatch stopwatch = StopWatch.createStarted();

    Model declaredPrefixes = rdfSources.peekDeclaredPrefixes();

    Map<String, String> usedPm = null;

    if (rdfSources.containsQuadLangs()) {

      JavaRDD<Quad> rdd = rdfSources.asQuads().toJavaRDD();
      rdd = JavaRddOfQuadsOps.postProcess(rdd, ppc.unique, !ppc.reverse, ppc.unique, ppc.numPartitions);

      if (cmd.postProcessConfig.optimizePrefixes && !declaredPrefixes.getNsPrefixMap().isEmpty()) {
        usedPm = JavaRddOps.aggregateUsingJavaCollector(
                rdd.flatMap(q -> QuadUtils.quadToList(q).iterator()),
                NodeAnalytics.usedPrefixes(declaredPrefixes.getNsPrefixMap()).asCollector());
        rddRdfWriterFactory.setGlobalPrefixMapping(usedPm);
      }
      rddRdfWriterFactory.forQuad(rdd).run();

    } else {

      JavaRDD<Triple> rdd = rdfSources.asTriples().toJavaRDD();
      rdd = JavaRddOfTriplesOps.postProcess(rdd, ppc.unique, !ppc.reverse, ppc.unique, ppc.numPartitions);

      if (cmd.postProcessConfig.optimizePrefixes && !declaredPrefixes.getNsPrefixMap().isEmpty()) {
        usedPm = JavaRddOps.aggregateUsingJavaCollector(
                rdd.flatMap(t -> TripleUtils.tripleToList(t).iterator()),
                NodeAnalytics.usedPrefixes(declaredPrefixes.getNsPrefixMap()).asCollector());
        rddRdfWriterFactory.setGlobalPrefixMapping(usedPm);
      }

      rddRdfWriterFactory.forTriple(rdd).run();

    }

    if (usedPm != null) {
      int usedPmCount = usedPm.size();
      logger.info(String.format("Optimization of prefixes reduced their number from %d to %d", initialPrefixes, usedPmCount));
    }

    logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");

    return 0; // exit code
  }
}

