package net.sansa_stack.examples.spark.ml.clustering

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import net.sansa_stack.ml.spark.clustering.BorderFlow

object BorderFlowClustering {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String): Unit = {

    val spark = SparkSession.builder
      .appName(s"BorderFlow example ( $input )")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("============================================")
    println("| Border Flow example                      |")
    println("============================================")

    BorderFlow(spark, input)

    spark.stop

  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("BorderFlow") {

    head("BorderFlow: an example BorderFlow app.")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file contains the input files")

    help("help").text("prints this usage text")
  }
}