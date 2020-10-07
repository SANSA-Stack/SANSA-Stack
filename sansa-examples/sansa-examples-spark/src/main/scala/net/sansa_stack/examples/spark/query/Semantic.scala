package net.sansa_stack.examples.spark.query

import net.sansa_stack.query.spark.semantic.QuerySystem
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.partition._
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession

/**
 * Run SPARQL queries over Spark using Semantic partitioning approach.
 *
 * @author Gezim Sejdiu
 */
object Semantic {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.queries)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, queries: String): Unit = {

    println("===========================================")
    println("| SANSA - Semantic Partioning example     |")
    println("===========================================")

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("SANSA - Semantic Partioning")
      .getOrCreate()

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)

    val partitionData = triples.partitionGraphAsSemantic()

    val result = new QuerySystem(partitionData, queries).run()
    result.take(5).foreach(println)

    spark.close()

  }

  case class Config(in: String = "", queries: String = "")

  val parser = new scopt.OptionParser[Config]("SANSA - Semantic Partioning example") {

    head(" SANSA - Semantic Partioning example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[String]('q', "queries").required().valueName("<directory>").
      action((x, c) => c.copy(queries = x)).
      text("path to the file containing the SPARQL query")

    help("help").text("prints this usage text")
  }

}
