package net.sansa_stack.examples.spark.owl

import net.sansa_stack.owl.spark.dataset.{FunctionalSyntaxOWLAxiomsDatasetBuilder, ManchesterSyntaxOWLAxiomsDatasetBuilder}
import org.apache.spark.sql.SparkSession

object OWLReaderDataset {

  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.syntax)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, syntax: String): Unit = {

    println(".============================================.")
    println("| Dataset OWL reader example (" + syntax + " syntax)|")
    println(".============================================.")

    val spark = SparkSession.builder
      .appName(s"Dataset OWL reader ( $input + )($syntax)")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "net.sansa_stack.owl.spark.dataset.UnmodifiableCollectionKryoRegistrator")
      .getOrCreate()

    val dataset = syntax match {
      case "fun" => FunctionalSyntaxOWLAxiomsDatasetBuilder.build(spark, input)
      case "manch" => ManchesterSyntaxOWLAxiomsDatasetBuilder.build(spark, input)
      case "owl_xml" =>
        throw new RuntimeException("'" + syntax + "' - Not supported, yet.")
      case _ =>
        throw new RuntimeException("Invalid syntax type: '" + syntax + "'")
    }

    dataset.take(10).foreach(println(_))
    spark.stop()
  }

  case class Config(
    in: String = "",
    syntax: String = "")

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("Dataset OWL reader example") {

    head("Dataset OWL reader example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data")

    opt[String]('s', "syntax").required().valueName("{fun | manch | owl_xml}").
      action((x, c) => c.copy(syntax = x)).
      text("the syntax format")

    help("help").text("prints this usage text")
  }
}
