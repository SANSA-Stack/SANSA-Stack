package net.sansa_stack.examples.spark.ml.clustering

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import org.apache.jena.riot.{ Lang, RDFDataMgr }
import java.io.ByteArrayInputStream
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io._
import net.sansa_stack.rdf.spark.model.graph._
import net.sansa_stack.ml.spark.clustering.RDFGraphPowerIterationClustering

object RDFGraphPIClustering {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out, config.outevl, config.outputsim, config.k, config.maxIterations)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, output: String, outevl: String, outputsim: String, k: Int, maxIterations: Int): Unit = {

    val spark = SparkSession.builder
      .appName(s"Power Iteration Clustering example ( $input )")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    System.setProperty("spark.akka.frameSize", "2000")
    

    println("============================================")
    println("| Power Iteration Clustering   example     |")
    println("============================================")

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)

    val graph = triples.asStringGraph()

    RDFGraphPowerIterationClustering(spark, graph, output, outevl, outputsim, k, maxIterations)

    spark.stop

  }

  case class Config(in: String = "", out: String = "", outevl: String = "", outputsim: String = "", k: Int = 2, maxIterations: Int = 50)

  val defaultParams = Config()

  val parser = new scopt.OptionParser[Config]("RDFGraphPIClustering") {

    head("PowerIterationClusteringExample: an example PIC app using concentric circles.")

    opt[String]('i', "input").required().valueName("<path>")
      .text(s"path to file that contains the input files (in N-Triple format)")
      .action((x, c) => c.copy(in = x))

    opt[String]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    opt[String]('e', "outevl").optional().valueName("<directory>").
      action((x, c) => c.copy(outevl = x)).
      text("the outputevl directory")

    opt[String]('s', "outputsim").optional().valueName("<directory>").
      action((x, c) => c.copy(outputsim = x)).
      text("the outputevl directory")
      

    opt[Int]('k', "k")
      .text(s"number of circles (/clusters), default: ${defaultParams.k}")
      .action((x, c) => c.copy(k = x))

    opt[Int]("maxIterations")
      .text(s"number of iterations, default: ${defaultParams.maxIterations}")
      .action((x, c) => c.copy(maxIterations = x))

    help("help").text("prints this usage text")
  }
}
