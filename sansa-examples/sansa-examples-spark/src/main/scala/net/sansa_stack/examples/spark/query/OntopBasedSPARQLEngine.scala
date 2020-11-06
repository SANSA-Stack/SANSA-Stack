package net.sansa_stack.examples.spark.query

import java.awt.Desktop
import java.net.URI

import org.aksw.jena_sparql_api.server.utils.FactoryBeanSparqlServer
import org.apache.jena.query.QueryFactory
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import net.sansa_stack.query.spark.ontop.{OntopSPARQLEngine, QueryExecutionFactoryOntopSpark}
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionComplex, RdfPartitionerComplex}
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark

/**
  * Run SPARQL queries over Spark using Ontop as SPARQL-to-SQL rewriter.
  */
object OntopBasedSPARQLEngine {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.sparql, config.run, config.port)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, sparqlQuery: String = "", run: String = "cli", port: Int = 7531): Unit = {

    println("======================================")
    println("|   Ontop based SPARQL example       |")
    println("======================================")

    val spark = SparkSession.builder
      .appName(s"Ontop SPARQL example ( $input )")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
      .getOrCreate()

    // load the data into an RDD
    val lang = Lang.NTRIPLES
    val data = spark.rdf(lang)(input)

    // apply vertical partitioning which is necessary for the current Ontop integration
    val partitions: Map[RdfPartitionComplex, RDD[Row]] = RdfPartitionUtilsSpark.partitionGraph(data, partitioner = RdfPartitionerComplex(false))

    // create the SPARQL engine
    val ontopEngine = OntopSPARQLEngine(spark, partitions, ontology = None)

    // run i) a single SPARQL query and terminate or ii) host some SNORQL web UI
    run match {
      case "cli" =>
        // only SELECT queries will be considered here
        val result = ontopEngine.execSelect(sparqlQuery)
        // show bindings on command line
        result.foreach(println)
      case "endpoint" =>
        val qef = new QueryExecutionFactoryOntopSpark(spark, ontopEngine)
        val server = FactoryBeanSparqlServer.newInstance.setSparqlServiceFactory(qef).setPort(port).create()
        if (Desktop.isDesktopSupported) {
          Desktop.getDesktop.browse(URI.create("http://localhost:" + port + "/sparql"))
        }
        server.join()
      case _ => // should never happen
    }

    spark.stop

  }

  case class Config(in: String = "",
                    sparql: String = "SELECT * WHERE {?s ?p ?o} LIMIT 10",
                    run: String = "cli",
                    port: Int = 7531)

  val parser = new scopt.OptionParser[Config]("Ontop SPARQL example") {

    head("Ontop SPARQL example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[String]('q', "sparql").optional().valueName("<query>").
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

    opt[String]('r', "run").optional().valueName("Runner").
      action((x, c) => c.copy(run = x)).
      validate(x => if (x == "cli" || x == "endpoint") success
                    else failure("wrong run mode: use either 'cli' or 'endpoint'")).
      text("Runner method ('cli', 'endpoint'). Default:'cli'")

    opt[Int]('p', "port").optional().valueName("port").
      action((x, c) => c.copy(port = x)).
      text("port that SPARQL endpoint will be exposed, default:'7531'")

    checkConfig(c =>
      if (c.run == "cli" && c.sparql.isEmpty) failure("Option --sparql must not be empty if cli is enabled")
      else success)

    help("help").text("prints this usage text")
  }

}
