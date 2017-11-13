package net.sansa_stack.examples.spark.rdf

import org.apache.spark.sql.SparkSession
import java.net.URI
import net.sansa_stack.rdf.spark.io.NTripleReader
import scala.collection.mutable
import java.io.File
import net.sansa_stack.rdf.spark.stats.RDFStatistics

object RDFStats {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, output: String): Unit = {

    val rdf_stats_file = new File(input).getName

    val spark = SparkSession.builder
      .appName(s"RDF Dataset Statistics example $rdf_stats_file")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println("|        RDF Statistic example       |")
    println("======================================")

    val triples = NTripleReader.load(spark, URI.create(input))

    // compute  criterias
    val rdf_statistics = RDFStatistics(triples, spark)
    val stats = rdf_statistics.run()
    rdf_statistics.voidify(stats, rdf_stats_file, output)
  }

  // the config object
  case class Config(
    in:  String = "",
    out: String = "")

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("RDF Dataset Statistics Example") {

    head("RDF Dataset Statistics Example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[String]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    help("help").text("prints this usage text")
  }
}