package net.sansa_stack.examples.spark.ml.kernel

import net.sansa_stack.ml.spark.kernel._
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession

/**
 * RDF Graph Kernel example.
 */
object RDFGraphKernel {
  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.iteration)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, iteration: Int = 5): Unit = {

    println("======================================")
    println("|        RDF Graph Kernel example     |")
    println("======================================")

    val spark = SparkSession.builder
      .appName(s" RDF Graph Kernel example ( $input )")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val t0 = System.nanoTime
    val lang = Lang.NTRIPLES

    val triples = spark.rdf(lang)(input)
      .filter(_.getPredicate.getURI != "http://swrc.ontoware.org/ontology#employs")

    val rdfFastGraphKernel = RDFFastGraphKernel(spark, triples, "http://swrc.ontoware.org/ontology#affiliation")
    val data = rdfFastGraphKernel.getMLLibLabeledPoints

    val t1 = System.nanoTime
    RDFFastTreeGraphKernelUtil.printTime("Initialization", t0, t1)

    RDFFastTreeGraphKernelUtil.predictLogisticRegressionMLLIB(data, 4, iteration)

    val t2 = System.nanoTime
    RDFFastTreeGraphKernelUtil.printTime("Run Prediction", t1, t2)

  }

  case class Config(
    in:        String = "",
    iteration: Int    = 5)

  val parser = new scopt.OptionParser[Config]("Mines the Rules example") {

    head("Mines the Rules example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data")

    opt[Int]('k', "iteration").required().valueName("<iteration>").
      action((x, c) => c.copy(iteration = x)).
      text("the iteration or folding on validation")

    help("help").text("prints this usage text")
  }
}

