package net.sansa_stack.examples.spark.ml.clustering

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import net.sansa_stack.ml.spark.clustering.BorderFlow

object BorderFlowClustering {
  def main(args: Array[String]) = {
    if (args.length < 1) {
      System.err.println(
        "Usage: BorderFlow <input> ")
      System.exit(1)
    }
    val input = args(0) //"src/main/resources/BorderFlow_Sample1.txt"
    val optionsList = args.drop(1).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _             => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)

    options.foreach {
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }
    println("============================================")
    println("| Border Flow example                      |")
    println("============================================")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName(" BorderFlow example (" + input + ")")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.ERROR)

    BorderFlow(spark, input)

    spark.stop
  }
}