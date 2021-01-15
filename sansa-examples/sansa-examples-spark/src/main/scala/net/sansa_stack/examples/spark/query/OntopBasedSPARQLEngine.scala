package net.sansa_stack.examples.spark.query

import java.awt.Desktop
import java.net.URI
import java.nio.file.Paths

import net.sansa_stack.query.spark.ontop.{OntopSPARQLEngine, PartitionSerDe, QueryExecutionFactorySparkOntop}
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitionerComplex}
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.aksw.jena_sparql_api.server.utils.FactoryBeanSparqlServer
import org.apache.jena.query.QueryFactory
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Run SPARQL queries over Spark using Ontop as SPARQL-to-SQL rewriter.
  */
object OntopBasedSPARQLEngine {

  JenaSystem.init

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.database, config.partitioningMetadataPath, config.sparql, config.runMode, config.port)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String,
          database: String,
          partitioningMetadataPath: URI,
          sparqlQuery: String = "",
          run: String = "cli",
          port: Int = 7531): Unit = {

    println("======================================")
    println("|   Ontop based SPARQL example       |")
    println("======================================")

    val spark = SparkSession.builder
      .appName(s"Ontop SPARQL example ( $input )")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator"))
      .config("spark.sql.crossJoin.enabled", true)
      .enableHiveSupport()
      .getOrCreate()

    val partitioner = RdfPartitionerComplex(false)

    val ontopEngine = if (input != null) {
      // load the data into an RDD
      val lang = Lang.NTRIPLES
      val data = spark.rdf(lang)(input)

      // apply vertical partitioning which is necessary for the current Ontop integration
      val partitions: Map[RdfPartitionStateDefault, RDD[Row]] = RdfPartitionUtilsSpark.partitionGraph(data, partitioner)

      // create the SPARQL engine
      OntopSPARQLEngine(spark, partitioner, partitions, ontology = None)
    } else {
      // load partitioning metadata
      val partitions = PartitionSerDe.deserializeFrom(Paths.get(partitioningMetadataPath))
      // create the SPARQL engine
      OntopSPARQLEngine(spark, database, partitioner, partitions, None)
    }

    // run i) a single SPARQL query and terminate or ii) host some SNORQL web UI
    run match {
      case "cli" =>
        // only SELECT queries will be considered here
        val result = ontopEngine.execSelect(sparqlQuery).collect()
        // show bindings on command line
        result.foreach(println)
      case "endpoint" =>
        val qef = new QueryExecutionFactorySparkOntop(spark, ontopEngine)
        val server = FactoryBeanSparqlServer.newInstance.setSparqlServiceFactory(qef).setPort(port).create()
        if (Desktop.isDesktopSupported) {
          Desktop.getDesktop.browse(URI.create("http://localhost:" + port + "/sparql"))
        }
        server.join()
      case _ => // should never happen
    }

    spark.stop

  }

  case class Config(in: String = null,
                    database: String = null,
                    partitioningMetadataPath: URI = null,
                    sparql: String = "SELECT * WHERE {?s ?p ?o} LIMIT 10",
                    runMode: String = "cli",
                    port: Int = 7531,
                    browser: Boolean = true)

  val parser = new scopt.OptionParser[Config]("Ontop SPARQL example") {

    head("Ontop SPARQL example")

    opt[String]('i', "input")
      .valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[URI]( "metadata")
      .action((x, c) => c.copy(partitioningMetadataPath = x))
      .text("path to partitioning metadata")

    opt[String]("database")
      .abbr("db")
      .action((x, c) => c.copy(database = x))
      .text("the name of the Spark database used as KB")

    opt[String]('q', "query").optional().valueName("<query>").
      action((x, c) => c.copy(sparql = x)).
      validate(x => try {
        QueryFactory.create(x)
        success
      } catch {
        case e: Exception =>
          e.printStackTrace()
          failure("Must be a valid SPARQL query.")
      }).
      text("a SPARQL query")

    opt[String]('m', "mode").optional().valueName("run mode").
      action((x, c) => c.copy(runMode = x)).
      validate(x => if (x == "cli" || x == "endpoint") success
                    else failure("wrong run mode: use either 'cli' or 'endpoint'")).
      text("Runner mode ('cli', 'endpoint'). Default:'cli'")

    opt[Int]('p', "port").optional().valueName("port").
      action((x, c) => c.copy(port = x)).
      text("port that SPARQL endpoint will be exposed, default:'7531'")

    checkConfig(c =>
      if (c.runMode == "cli" && c.sparql.isEmpty) failure("Option --sparql must not be empty if cli is enabled")
      else success)

    help("help").text("prints this usage text")
  }

}
