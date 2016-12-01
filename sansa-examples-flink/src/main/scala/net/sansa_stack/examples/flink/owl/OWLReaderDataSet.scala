package net.sansa_stack.examples.flink.owl


import net.sansa_stack.owl.flink.dataset.{FunctionalSyntaxOWLAxiomsDataSetBuilder, ManchesterSyntaxOWLAxiomsDataSetBuilder}
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable

object OWLReaderDataSet {
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

        val env = ExecutionEnvironment.getExecutionEnvironment

        val dataSet = FunctionalSyntaxOWLAxiomsDataSetBuilder.build(env, input)
        dataSet.first(10).print()

      case "manch" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println(".================================================.")
        println("| Dataset OWL reader example (Manchester syntax) |")
        println("`================================================´")

        val env = ExecutionEnvironment.getExecutionEnvironment

        val dataSet = ManchesterSyntaxOWLAxiomsDataSetBuilder.build(env, input)
        dataSet.first(10).print()

      case "owl_xml" =>
        println("Not supported, yet.")

      case _ =>
        println("Invalid syntax type.")
    }
  }
}
