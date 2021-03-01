package net.sansa_stack.spark.cli.cmd.impl

import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.query.spark.SPARQLEngine.{Ontop, Sparqlify}
import net.sansa_stack.query.spark.ontop.QueryEngineFactoryOntop
import net.sansa_stack.query.spark.sparqlify.QueryEngineFactorySparqlify
import net.sansa_stack.spark.cli.cmd.CmdSansaEndpoint
import org.aksw.jena_sparql_api.server.utils.FactoryBeanSparqlServer
import org.apache.jena.graph
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}

/**
 * Called from the Java class [[CmdSansaEndpoint]]
 */
class CmdSansaEndpointImpl
object CmdSansaEndpointImpl {
  private val logger = LoggerFactory.getLogger(getClass)

  def run(cmd: CmdSansaEndpoint): Integer = {

    import collection.JavaConverters._

    val spark = SparkSession.builder
      .master(cmd.sparkMaster)
      .appName(s"SANSA SPARQL Endpoint")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
      .config("spark.sql.crossJoin.enabled", true)
      .getOrCreate()

    import net.sansa_stack.rdf.spark.io._

    // we build the query engine here
    val queryEngineFactory = SPARQLEngine.withName(cmd.queryEngine) match {
      case Ontop => new QueryEngineFactoryOntop(spark)
      case Sparqlify => new QueryEngineFactorySparqlify(spark)
      case _ => throw new RuntimeException("Unsupported query engine")
    }

    // create the query execution factory
    val qef =
      if (cmd.dataset.preloadedDatasetArgs != null) { // pre-partitioned case
        val dataset = cmd.dataset.preloadedDatasetArgs
        // load the R2RML mappings
        val mappings = ModelFactory.createDefaultModel()
        RDFDataMgr.read(mappings, Paths.get(dataset.mappingsFile).toAbsolutePath.toString)

        queryEngineFactory.create(Some(dataset.database), mappings)
      } else {
        val dataset = cmd.dataset.freshDatasetArgs

        val triplesFiles = dataset.triplesFile.asScala
          .map(pathStr => Paths.get(pathStr).toAbsolutePath)
          .toList

        val validPaths = triplesFiles
          .filter(Files.exists(_))
          .filter(!Files.isDirectory(_))
          .filter(Files.isReadable)
          .toSet

        val invalidPaths = triplesFiles.toSet.diff(validPaths)
        if (invalidPaths.nonEmpty) {
          throw new IllegalArgumentException("The following paths are invalid (do not exist or are not a (readable) file): " + invalidPaths)
        }

        // load the data into an RDD
        var triplesRDD: RDD[graph.Triple] = spark.rdf(validPaths.map(_.toString).mkString(","))

        if (dataset.makeDistinct) triplesRDD = triplesRDD.distinct()

        queryEngineFactory.create(triplesRDD)
      }

    // run i) a single SPARQL query and terminate or ii) host some SNORQL web UI
    val server = FactoryBeanSparqlServer.newInstance.setSparqlServiceFactory(qef).setPort(cmd.port).create()
    server.join()

    0 // exit code
  }
}

