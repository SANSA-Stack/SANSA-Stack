package net.sansa_stack.examples.spark.query

import net.sansa_stack.query.spark._
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession

object HDTQuery {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.query)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, query: String): Unit = {

    println("===========================================")
    println("| SANSA - HDT example                     |")
    println("===========================================")

    val spark = SparkSession.builder
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("SANSA - HDT")
      .getOrCreate()

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)

    val triples_hdt = triples.asHDT()

    val result = triples_hdt.sparqlHDT(query)

    result.show()

    spark.close()

  }

  case class Config(in: String = "", query: String = "")

  val parser = new scopt.OptionParser[Config]("SANSA - HDT example") {

    head(" SANSA - HDT example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[String]('q', "query").required().valueName("SPARQL query").
      action((x, c) => c.copy(query = x)).
      text("the SPARQL query")

    help("help").text("prints this usage text")
  }

}
