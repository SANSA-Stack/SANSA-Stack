package net.sansa_stack.examples.spark.owl

import net.sansa_stack.owl.spark.dataset.{FunctionalSyntaxOWLAxiomsDatasetBuilder, ManchesterSyntaxOWLAxiomsDatasetBuilder}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object OWLReaderDataset {
  def main(args: Array[String]) = {
    if (args.length < 1) {
      System.err.println(
        "Usage: Dataset OWL reader <input> <syntax>")
      System.err.println("Supported 'Syntax' as follows:")
      System.err.println("  fun               Functional syntax")
      System.err.println("  manch             Manchester syntax")
      System.err.println("  owl_xml           OWL/XML")
      System.exit(1)
    }
    val input = args(0)
    val syntax = args(1)
    val optionsList = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _             => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)

    syntax match {
      case "fun" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println(".================================================.")
        println("| Dataset OWL reader example (Functional syntax) |")
        println("`================================================´")

        val sparkSession = SparkSession.builder
          .master("local[*]")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .appName("Dataset reader example (" + input + ")(Functional syntax)")
          .getOrCreate()

        val dataset = FunctionalSyntaxOWLAxiomsDatasetBuilder.build(sparkSession, input)
        dataset.take(10).foreach(println(_))

        sparkSession.stop

      case "manch" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println(".================================================.")
        println("| Dataset OWL reader example (Manchester syntax) |")
        println("`================================================´")

        val sparkSession = SparkSession.builder
          .master("local[*]")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .appName("Dataset reader example (" + input + ")(Manchester syntax)")
          .getOrCreate()

        val rdd = ManchesterSyntaxOWLAxiomsDatasetBuilder.build(sparkSession, input)
        rdd.take(10).foreach(println(_))

        sparkSession.stop

      case "owl_xml" =>
        println("Not supported, yet.")

      case _ =>
        println("Invalid syntax type.")
    }
  }
}
