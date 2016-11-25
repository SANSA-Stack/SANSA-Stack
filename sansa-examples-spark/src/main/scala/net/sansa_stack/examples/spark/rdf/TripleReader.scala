package net.sansa_stack.examples.spark.rdf

import java.io.File
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.model.JenaSparkRDDOps

object TripleReader {

  def main(args: Array[String]) = {
    if (args.length < 2) {
      System.err.println(
        "Usage: Triple reader <input> <output>")
      System.exit(1)
    }
    val input = args(0)
    val output = args(1)
    val optionsList = args.drop(2).map { arg =>
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
      .appName("Triple reader example (" + input + ")")
      .getOrCreate()

    val ops = JenaSparkRDDOps(sparkSession.sparkContext)
    import ops._

    val triples = fromNTriples(input, "http://dbpedia.org").toSeq
    val triplesRDD = sparkContext.parallelize(triples)

 /*   val counts = triplesRDD.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)*/

    triplesRDD.saveAsTextFile(output)

    sparkSession.stop

  }

}