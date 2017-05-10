package net.sansa_stack.examples.spark.rdf

import java.net.URI

import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object TripleReader {

  def main(args: Array[String]) = {
    if (args.length < 1) {
      System.err.println(
        "Usage: Triple reader <input>")
      System.exit(1)
    }
    val input = args(0)
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
    println("======================================")
    println("|        Triple reader example       |")
    println("======================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple reader example (" + input + ")")
      .getOrCreate()

    val triplesRDD = NTripleReader.load(sparkSession, URI.create(input))

    triplesRDD.take(5).foreach(println(_))

    //triplesRDD.saveAsTextFile(output)

    sparkSession.stop

  }

}