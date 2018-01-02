package net.sansa_stack.examples.spark.ml.clustering

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import net.sansa_stack.ml.spark.clustering.{BorderFlow,FirstHardeninginBorderFlow}

object BorderFlowClustering {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.alg)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, algName:String): Unit = {

    val spark = SparkSession.builder
      .appName(s"BorderFlow example ( $input )")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("============================================")
    println("| Border Flow example                      |")
    println("============================================")
    
    val borderflow = algName match {
      case "borderflow"       => BorderFlow(spark, input)
      case "firsthardening" => FirstHardeninginBorderFlow(spark, input)
      case _ =>
        throw new RuntimeException("'" + algName + "' - Not supported, yet.")
    }

    spark.stop

  }

  case class Config(in: String = "", alg:String ="borderflow")

  val parser = new scopt.OptionParser[Config]("BorderFlow") {

    head("BorderFlow: an example BorderFlow app.")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file contains the input files")
      
    opt[String]('a', "algName").required().valueName("{borderflow | firsthardening }").
      action((x, c) => c.copy(alg = x)).
      text("BorderFlow algorithm type")

    help("help").text("prints this usage text")
  }
}