package net.sansa_stack.examples.spark.ml.clustering

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import net.sansa_stack.ml.spark.clustering.{ BorderFlow, FirstHardeninginBorderFlow }
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import net.sansa_stack.rdf.spark.model.graph._

object BorderFlowClustering {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.alg, config.in, config.out, config.outevlsoft, config.outevlhard)
      case None =>
        println(parser.usage)
    }
  }

  def run(algName: String, input: String, output: String, outputevlsoft: String, outputevlhard: String): Unit = {

    val spark = SparkSession.builder
      .appName(s"BorderFlow example: $algName ( $input )")
      .master("local[*]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("============================================")
    println(s"| Border Flow example    ($algName)       |")
    println("============================================")

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)
    val graph = triples.asStringGraph()

    val borderflow = algName match {
      case "borderflow"     => BorderFlow(spark, graph, output, outputevlsoft, outputevlhard)
      case "firsthardening" => FirstHardeninginBorderFlow(spark, graph, output, outputevlhard)
      case _ =>
        throw new RuntimeException("'" + algName + "' - Not supported, yet.")
    }

    spark.stop

  }

  case class Config(alg: String = "borderflow", in: String = "", out: String = "", outevlsoft: String = "", outevlhard: String = "")

  val parser = new scopt.OptionParser[Config]("BorderFlow") {

    head("BorderFlow: an example BorderFlow app.")

    opt[String]('a', "algName").required().valueName("{borderflow | firsthardening }").
      action((x, c) => c.copy(alg = x)).
      text("BorderFlow algorithm type")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file contains the input files")

    opt[String]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    opt[String]('e', "outevlsoft").optional().valueName("<directory>").
      action((x, c) => c.copy(outevlsoft = x)).
      text("the outevlsoft directory (used only for alg 'borderflow')")

    opt[String]('h', "outevlhard").required().valueName("<directory>").
      action((x, c) => c.copy(outevlhard = x)).
      text("the outevlhard directory ")

    help("help").text("prints this usage text")
    checkConfig(c =>
      if (c.alg == "borderflow" && c.outevlsoft.isEmpty) failure("Option --outevlsoft must not be empty if alg 'borderflow' is set")
      else success)
  }
}
