package net.sansa_stack.examples.spark.ml.clustering

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import net.sansa_stack.ml.spark.clustering.{ RDFByModularityClustering => RDFByModularityClusteringAlg }

object RDFByModularityClustering {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out, config.numIterations)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, output: String, numIterations: Int): Unit = {

    val spark = SparkSession.builder
      .appName(s"RDF By Modularity Clustering example example ( $input )")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("============================================")
    println("| RDF By Modularity Clustering example     |")
    println("============================================")

    Logger.getRootLogger.setLevel(Level.ERROR)

    RDFByModularityClusteringAlg(spark.sparkContext, numIterations, input, output)

    spark.stop

  }

  case class Config(in: String = "", out: String = "", numIterations: Int = 100)

  val defaultParams = Config()

  val parser = new scopt.OptionParser[Config]("RDF By Modularity Clustering") {

    head("RDF By Modularity Clustering: an example RDF By Modularity Clustering app using RDF Graph.")

    opt[String]('i', "input").required().valueName("<path>")
      .text(s"path to file that contains the input files (in N-Triple format)")
      .action((x, c) => c.copy(in = x))

    opt[String]('o', "output").valueName("<directory>")
      .text("the output directory")
      .action((x, c) => c.copy(out = x))

    opt[Int]("numIterations")
      .text(s"number of iterations, default: ${defaultParams.numIterations}")
      .action((x, c) => c.copy(numIterations = x))

    help("help").text("prints this usage text")
  }
}