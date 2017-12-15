package net.sansa_stack.examples.spark.query

import java.net.URI

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
import net.sansa_stack.query.spark.sparqlify.QueryExecutionFactorySparqlifySpark
import net.sansa_stack.query.spark.sparqlify.SparqlifyUtils3
import org.aksw.jena_sparql_api.server.utils.FactoryBeanSparqlServer
import org.apache.spark.sql.SparkSession
import java.awt.Desktop

import scala.collection.mutable
import java.awt.Desktop

/*
 * Run SPARQL queries over Spark using Sparqlify approach.
 */
object Sparklify {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        println(parser.usage)
    }
  }
  def run(input: String): Unit = {

    println("======================================")
    println("|   Sparklify example                |")
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

    val graphRdd = NTripleReader.load(spark, URI.create(input))

    val partitions = RdfPartitionUtilsSpark.partitionGraph(graphRdd)
    val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(spark, partitions)

    val port = 7531

    val qef = new QueryExecutionFactorySparqlifySpark(spark, rewriter)
    val server = FactoryBeanSparqlServer.newInstance.setSparqlServiceFactory(qef).setPort(port).create()

    if (Desktop.isDesktopSupported()) {
      Desktop.getDesktop().browse(new URI("http://localhost:" + port + "/sparql"));
    }

    server.join()
    spark.stop

  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("Sparklify example") {

    head(" Sparklify example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")
    help("help").text("prints this usage text")
  }

}
