package net.sansa_stack.examples.spark.query

import java.net.URI
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import net.sansa_stack.query.spark.sparqlify.{ QueryExecutionFactorySparqlifySpark, QueryExecutionUtilsSpark, QueryExecutionSpark, SparqlifyUtils3 }
import org.aksw.jena_sparql_api.server.utils.FactoryBeanSparqlServer
import org.apache.spark.sql.SparkSession
import java.awt.Desktop

import scala.collection.mutable

/*
 * Run SPARQL queries over Spark using Sparqlify approach.
 */
object Sparqlify {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.sparql, config.endpoint, config.port)
      case None =>
        println(parser.usage)
    }
  }
  def run(input: String, sparqlQuery: String = "", endpoint: Boolean = true, port: String = "7531"): Unit = {

    println("======================================")
    println("|   Sparqlify example                |")
    println("======================================")

    val spark = SparkSession.builder
      .appName(s"Sparklify example ( $input )")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
      .getOrCreate()

    val lang = Lang.NTRIPLES
    val graphRdd = spark.rdf(lang)(input)

    val checkendpoint = endpoint match {
      case j if(endpoint) =>
        val partitions = RdfPartitionUtilsSpark.partitionGraph(graphRdd)
        val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(spark, partitions)

        val port = 7531

        val qef = new QueryExecutionFactorySparqlifySpark(spark, rewriter)
        val server = FactoryBeanSparqlServer.newInstance.setSparqlServiceFactory(qef).setPort(port).create()
        if (Desktop.isDesktopSupported()) {
          Desktop.getDesktop().browse(new URI("http://localhost:" + port + "/sparql"));
        }
        server.join()
      case _ =>
        import net.sansa_stack.query.spark.query._
        //val sparqlQuery = "SELECT * WHERE {?s ?p ?o} LIMIT 10"
        val result = graphRdd.sparql(sparqlQuery)
        result.rdd.foreach(println)
    }

    spark.stop

  }

  case class Config(in: String = "", sparql: String = "", endpoint: Boolean = true, port: String = "7531")

  val parser = new scopt.OptionParser[Config]("Sparqlify example") {

    head(" Sparqlify example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[String]('q', "sparql").optional().valueName("query").
      action((x, c) => c.copy(sparql = x)).
      text("a SPARQL query")

    opt[Boolean]('e', "endpoint").optional().valueName("SPARQL endoint enabled").
      action((x, c) => c.copy(endpoint = x)).
      text("enable SPARQL endpoint , default:'enabled'")

    opt[String]('p', "port").optional().valueName("port").
      action((x, c) => c.copy(port = x)).
      text("port that SPARQL endpoint will be exposed, default:'7531'")

    checkConfig(c =>
      if (c.endpoint == false && c.sparql.isEmpty) failure("Option --sparql must not be empty if endpoint is disabled")
      else success)

    checkConfig(c =>
      if (c.endpoint == true && c.port.isEmpty) failure("Option --port ust not be empty if endpoint is enabled")
      else success)

    help("help").text("prints this usage text")
  }

}
