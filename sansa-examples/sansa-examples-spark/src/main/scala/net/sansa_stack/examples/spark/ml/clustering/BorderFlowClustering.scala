package net.sansa_stack.examples.spark.ml.clustering

import scala.collection.mutable

import net.sansa_stack.ml.spark.clustering._
import net.sansa_stack.ml.spark.clustering.algorithms.BorderFlow
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model._
import org.apache.jena.riot.Lang
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession

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
      .appName(s"BorderFlow example: ( $input )")
      .master("local[*]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("============================================")
    println(s"| Border Flow example                     |")
    println("============================================")

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)

    val borderflow = triples.cluster(ClusteringAlgorithm.BorderFlow).asInstanceOf[BorderFlow].run()

    borderflow.collect().foreach(println)

    spark.stop

  }

  case class Config(in: String = "")

  val parser = new scopt.OptionParser[Config]("BorderFlow") {

    head("BorderFlow: an example BorderFlow app.")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file contains the input files")

  }
}
