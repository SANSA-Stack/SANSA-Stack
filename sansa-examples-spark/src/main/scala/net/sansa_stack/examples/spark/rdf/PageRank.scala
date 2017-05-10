package net.sansa_stack.examples.spark.rdf

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.JenaSparkGraphXOps
import org.apache.spark.sql.SparkSession
import java.net.{URI => JavaURI}


import scala.collection.mutable

/*
 * Computes the PageRank of Resources from an input .nt file.
 */
object PageRank {

  def main(args: Array[String]) = {
    if (args.length < 1) {
      System.err.println(
        "Usage: Resource PageRank <input>")
      System.exit(1)
    }
    val input = args(0) //"src/main/resources/rdf.nt"
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
    println("|   PageRank of resources example    |")
    println("======================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
      .appName("Resource PageRank example (" + input + ")")
      .getOrCreate()

    val ops = JenaSparkGraphXOps(sparkSession.sparkContext)
    import ops._

    val triplesRDD = NTripleReader.load(sparkSession, JavaURI.create(input))

    val graph = makeGraph(triplesRDD)
    val pagerank = graph.pageRank(0.00001).vertices
    val report = pagerank.join(graph.vertices)
      .map({ case (k, (r, v)) => (r, v, k) })
      .sortBy(50 - _._1)

    report.take(50).foreach(println)

    sparkSession.stop

  }

}