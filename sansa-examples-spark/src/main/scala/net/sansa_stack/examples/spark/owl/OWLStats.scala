package net.sansa_stack.examples.spark.owl

import java.io.File

import org.apache.spark.sql.SparkSession

import net.sansa_stack.owl.spark.owl._
import net.sansa_stack.owl.spark.owl.Syntax
import net.sansa_stack.owl.spark.stats.OWLStats

object OWLStats {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, output: String): Unit = {

    val owl_stats_file = new File(input).getName

    val spark = SparkSession.builder
      .appName(s"OWL Dataset Statistics example $owl_stats_file")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println("|        OWL Statistic example       |")
    println("======================================")

    val syntax = Syntax.FUNCTIONAL
    val axioms = spark.owl(syntax)(input)

    // compute  criteria
    val stats = new OWLStats(spark).run(axioms)

  }

  // the config object
  case class Config(in: String = "", out: String = "")

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("OWL Dataset Statistics Example") {

    head("OWL Dataset Statistics Example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    help("help").text("prints this usage text")
  }

}
