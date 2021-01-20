package net.sansa_stack.examples.spark.query

import java.awt.Desktop
import java.net.URI

import net.sansa_stack.query.spark.sparqlify.{JavaQueryExecutionFactorySparqlifySpark, SparqlifyUtils3}
import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import org.aksw.jena_sparql_api.server.utils.FactoryBeanSparqlServer
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql.SparkSession

/**
  * Run SPARQL queries over Spark using Sparqlify engine.
  *
  * Note: To run this class outside of spark-submit (e.g. from an IDE) you can specify a
  * spark master using a JVM argument: -Dspark.master=local[*]
  */
object Sparklify {

  JenaSystem.init

  case class Config(
                     dataFilenameOrUri: String = "",
                     queryString: String = "SELECT * WHERE {?s ?p ?o} LIMIT 10",
                     run: String = "cli",
                     port: Int = 7531)

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config)
      case None =>
        println(parser.usage)
    }
  }

  def run(config : Config): Unit = {

    println("======================================")
    println("|   Sparklify example                |")
    println("======================================")

    val spark = SparkSession.builder
      .appName(s"Sparklify example ( ${config.dataFilenameOrUri} )")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
      .getOrCreate()

    import net.sansa_stack.query.spark._
    val lang = Lang.NTRIPLES
    val triplesRDD = spark.rdf(lang)(config.dataFilenameOrUri)
    val qef = triplesRDD.sparql()

    config.run match {
      case "cli" =>
        val qe = qef.createQueryExecution(config.queryString)
        val result = qe.execSelectSpark()
        result.getBindings.foreach(println)
      case _ =>
        val server = FactoryBeanSparqlServer.newInstance.setSparqlServiceFactory(qef).setPort(config.port).create()
        if (Desktop.isDesktopSupported) {
          Desktop.getDesktop.browse(URI.create("http://localhost:" + config.port + "/sparql"))
        }
        server.join()
    }

    spark.stop

  }

  val parser = new scopt.OptionParser[Config]("Sparklify example") {

    head(" Sparqlify example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(dataFilenameOrUri = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[String]('q', "sparql").optional().valueName("<query>").
      action((x, c) => c.copy(queryString = x)).
      text("a SPARQL query")

    opt[String]('r', "run").optional().valueName("Runner").
      action((x, c) => c.copy(run = x)).
      text("Runner method, default:'cli'")

    opt[Int]('p', "port").optional().valueName("port").
      action((x, c) => c.copy(port = x)).
      text("port that SPARQL endpoint will be exposed, default:'7531'")

    checkConfig(c =>
      if (c.run == "cli" && c.queryString.isEmpty) failure("Option --sparql must not be empty if cli is enabled")
      else success)

    help("help").text("prints this usage text")
  }

}
