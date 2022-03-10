package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.spark.cli.cmd.CmdSansaSort;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactoryImpl;
import net.sansa_stack.spark.io.rdf.output.RddRdfWriterFactory;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfQuadsOps;
import net.sansa_stack.spark.rdd.op.rdf.JavaRddOfTriplesOps;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Called from the Java class [[CmdSansaSort]]
 */
public class CmdSansaSortImpl {
  private static Logger logger = LoggerFactory.getLogger(CmdSansaSortImpl.class);

  public static int run(CmdSansaSort cmd) throws IOException {

    List<String> inputStrs = cmd.inputFiles;

    SparkSession sparkSession = CmdUtils.newDefaultSparkSessionBuilder()
            .appName("Sansa Sort (" + cmd.inputFiles + ")")
            .getOrCreate();

    Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();

    RddRdfWriterFactory rddRdfWriterFactory = CmdUtils.configureWriter(cmd.outputConfig);

    CmdUtils.validatePaths(inputStrs, hadoopConf);

    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

    RdfSourceFactory rdfSourceFactory = RdfSourceFactoryImpl.from(sparkSession);

    boolean isQuads = false;
    List<RdfSource> rdfSources = new ArrayList<>();
    for (String input : cmd.inputFiles) {
      RdfSource rdfSource = rdfSourceFactory.get(input);
      Lang lang = rdfSource.getLang();
      if (RDFLanguages.isQuads(lang)) {
        isQuads = true;
      }
      rdfSources.add(rdfSource);
    }

    StopWatch stopwatch = StopWatch.createStarted();

    if (isQuads) {
      JavaRDD<Quad> rdd = javaSparkContext.union(
              rdfSources.stream()
                .map(source -> source.asQuads().toJavaRDD())
                .collect(Collectors.toList())
              .toArray(new JavaRDD[0]));

      rdd = JavaRddOfQuadsOps.sort(rdd, !cmd.reverse, cmd.unique, cmd.numPartitions);

      rddRdfWriterFactory.forQuad(rdd).run();
    } else {
      JavaRDD<Triple> rdd = javaSparkContext.union(
              rdfSources.stream()
                      .map(source -> source.asTriples().toJavaRDD())
                      .collect(Collectors.toList())
                      .toArray(new JavaRDD[0]));

      rdd = JavaRddOfTriplesOps.sort(rdd, !cmd.reverse, cmd.unique, cmd.numPartitions);

      rddRdfWriterFactory.forTriple(rdd).run();
    }

    logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");

    return 0; // exit code
  }
}

