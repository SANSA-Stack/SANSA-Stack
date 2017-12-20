package net.sansa_stack.examples.spark.owl

import net.sansa_stack.owl.spark.rdd.{ FunctionalSyntaxOWLAxiomsRDDBuilder, ManchesterSyntaxOWLAxiomsRDDBuilder }
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object OWLReaderRDD {

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
    println("| RDD OWL reader example (" + syntax + " syntax)|")
    println("`============================================´")

    val spark = SparkSession.builder
      .appName(s"OWL reader example ( $input + )($syntax)")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "net.sansa_stack.owl.spark.dataset.UnmodifiableCollectionKryoRegistrator")      
      .getOrCreate()

    val rdd = syntax match {
      case "fun"   => FunctionalSyntaxOWLAxiomsRDDBuilder.build(spark.sparkContext, input)
      case "manch" => ManchesterSyntaxOWLAxiomsRDDBuilder.build(spark.sparkContext, input)
      case "owl_xml" =>
        throw new RuntimeException("'" + syntax + "' - Not supported, yet.")
      case _ =>
        throw new RuntimeException("Invalid syntax type: '" + syntax + "'")
    }

    rdd.take(10).foreach(println(_))

  }

  case class Config(
    in:     String = "",
    syntax: String = "")

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("RDD OWL reader example") {

    head("RDD OWL reader example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data")

    opt[String]('s', "syntax").required().valueName("{fun | manch | owl_xml}").
      action((x, c) => c.copy(syntax = x)).
      text("the syntax format")

    help("help").text("prints this usage text")
  }
}
