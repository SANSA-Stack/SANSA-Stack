package net.sansa_stack.examples.spark.ml.clustering

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import java.net.{ URI => JavaURI }
import net.sansa_stack.ml.spark.clustering.{SilviaClustering => AlgSilviaClustering}

object SilviaClustering {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out, config.outeval)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, output: String, outputeval: String): Unit = {

    val spark = SparkSession.builder
      .appName(s"SilviaClustering example ( $input )")
      .master("local[*]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("============================================")
    println("| Silvia Clustering example                      |")
    println("============================================")

    Logger.getRootLogger.setLevel(Level.WARN)

    AlgSilviaClustering(spark, input, output, outputeval)

    spark.stop

  }

  case class Config(in: String = "", out: String = "", outeval: String = "")

  val parser = new scopt.OptionParser[Config]("SilviaClustering") {

    head("SilviaClustering: an example SilviaClustering app.")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file contains the input files")

    opt[String]('o', "output").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    opt[String]('e', "outputeval").required().valueName("<directory>").
      action((x, c) => c.copy(outeval = x)).
      text("the output evaluation directory")

    help("help").text("prints this usage text")
  }
}
