package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.rdf.spark.rdd.op.RddOfQuadsOps;
import net.sansa_stack.spark.cli.cmd.CmdMixinSparkPostProcess;
import net.sansa_stack.spark.cli.cmd.CmdSansaMap;
import net.sansa_stack.spark.cli.cmd.CmdSansaPrefixes;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceCollection;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactoryImpl;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfQuadsOps;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfTriplesOps;
import org.aksw.jenax.arq.analytics.NodeAnalytics;
import org.aksw.jenax.arq.util.quad.QuadUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Called from the Java class {@link CmdSansaMap}
 */
public class CmdSansaPrefixesImpl {
  private static Logger logger = LoggerFactory.getLogger(CmdSansaPrefixesImpl.class);

  public static int run(CmdSansaPrefixes cmd) throws IOException {

    List<String> inputStrs = cmd.inputFiles;

    SparkSession sparkSession = CmdUtils.newDefaultSparkSessionBuilder()
            .appName("Sansa Sort (" + cmd.inputFiles + ")")
            .getOrCreate();

    Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();

    CmdUtils.validatePaths(inputStrs, hadoopConf);

    RdfSourceFactory rdfSourceFactory = RdfSourceFactoryImpl.from(sparkSession);

    RdfSourceCollection rdfSources = CmdUtils.createRdfSourceCollection(rdfSourceFactory, cmd.inputFiles, cmd.inputConfig);


    StopWatch stopwatch = StopWatch.createStarted();

    // TODO FINISH THIS (Annoyingly we need to get the reference prefix set from the input format
    // so there is a bit of plumbing to do)
    // Collector<Map<String, String>> prefixCollector = NodeAnalytics.usedPrefixes();

    if (rdfSources.containsQuadLangs()) {


/*
      JavaRDD<Quad> rdd = rdfSources.asQuads().toJavaRDD()
              .flatMap(quad -> QuadUtils.quadToList(quad).iterator());
*/

    } else {
      JavaRDD<Triple> rdd = rdfSources.asTriples().toJavaRDD();
    }

    logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");

    return 0; // exit code
  }
}

