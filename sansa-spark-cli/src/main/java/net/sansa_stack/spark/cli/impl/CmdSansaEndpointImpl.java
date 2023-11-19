package net.sansa_stack.spark.cli.impl;

import net.sansa_stack.query.spark.api.domain.QueryEngineFactory;
import net.sansa_stack.query.spark.api.domain.QueryExecutionFactorySpark;
import net.sansa_stack.query.spark.ontop.QueryEngineFactoryOntop;
import net.sansa_stack.query.spark.sparqlify.QueryEngineFactorySparqlify;
import net.sansa_stack.spark.cli.cmd.CmdSansaEndpoint;
import net.sansa_stack.spark.cli.cmd.CmdSansaEndpoint.FreshDatasetArgs;
import net.sansa_stack.spark.cli.cmd.CmdSansaEndpoint.PreloadedDatasetArgs;
import net.sansa_stack.spark.cli.util.SansaCmdUtils;
import net.sansa_stack.spark.io.rdf.input.api.RdfSource;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceCollection;
import net.sansa_stack.spark.io.rdf.input.api.RdfSourceFactory;
import net.sansa_stack.spark.io.rdf.input.impl.RdfSourceFactoryImpl;
import org.aksw.jenax.web.server.boot.FactoryBeanSparqlServer;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Called from the Java class [[CmdSansaEndpoint]]
 */
public class CmdSansaEndpointImpl {
  private static final Logger logger = LoggerFactory.getLogger(CmdSansaEndpointImpl.class);

  public static int run(CmdSansaEndpoint cmd) throws InterruptedException {
    
    SparkSession sparkSession = SansaCmdUtils.newDefaultSparkSessionBuilder()
            .appName("Sansa SPARQL Endpoint")
            .config("spark.kryo.registrator", String.join(", ", 
                    "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
                    "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
            .config("spark.sql.crossJoin.enabled", true)
            .getOrCreate();

    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

    StopWatch stopwatch = StopWatch.createStarted();

    // we build the query engine here
    QueryEngineFactory engineFactory;
    String requestedQueryEngineName = Optional.ofNullable(cmd.queryEngine).map(String::toLowerCase).orElse(null);
    switch (requestedQueryEngineName) {
      case "ontop": engineFactory = new QueryEngineFactoryOntop(sparkSession); break;
      case "sparqlify": engineFactory = new QueryEngineFactorySparqlify(sparkSession); break;
      default: throw new RuntimeException("Unsupported query engine: " + requestedQueryEngineName);
    }

    // create the query execution factory
    QueryExecutionFactorySpark qef;
      if (cmd.dataset.preloadedDatasetArgs != null) { // pre-partitioned case
        PreloadedDatasetArgs dataset = cmd.dataset.preloadedDatasetArgs;
        // load the R2RML mappings
        Model mappings = ModelFactory.createDefaultModel();
        RDFDataMgr.read(mappings, Path.of(dataset.mappingsFile).toAbsolutePath().toString());

        qef = engineFactory.create(dataset.database, mappings);
      } else {
        FreshDatasetArgs dataset = cmd.dataset.freshDatasetArgs;

        RdfSourceFactory rdfSourceFactory = RdfSourceFactoryImpl.from(sparkSession);

        RdfSourceCollection sources = rdfSourceFactory.newRdfSourceCollection();
        for (String input : dataset.triplesFile) {
          RdfSource rdfSource = rdfSourceFactory.get(input);
          sources.add(rdfSource);
        }

        RDD<Triple> rdd = sources.asTriples();

        if (dataset.makeDistinct) {
          rdd = rdd.distinct();
        }

        qef = engineFactory.create(rdd);
      }

    logger.info("Processing time: " + stopwatch.getTime(TimeUnit.SECONDS) + " seconds");

    // run i) a single SPARQL query and terminate or ii) host some SNORQL web UI
    Server server = FactoryBeanSparqlServer.newInstance()
            .setSparqlServiceFactory(qef)
            .setPort(cmd.port)
            .create();
    server.join();

    return 0; // exit code
  }
}

