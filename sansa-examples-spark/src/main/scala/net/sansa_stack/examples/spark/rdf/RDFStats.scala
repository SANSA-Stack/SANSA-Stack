package net.sansa_stack.examples.spark.rdf

import org.apache.spark.sql.SparkSession
import java.net.URI
import net.sansa_stack.rdf.spark.io.NTripleReader
import scala.collection.mutable
import java.io.File
import net.sansa_stack.rdf.spark.stats.RDFStatistics

object RDFStats {
  def main(args: Array[String]) = {
   /* if (args.length < 2) {
      System.err.println(
        "Usage: RDF Statistics <input> <output>")
      System.exit(1)
    }*/
    val input = "src/main/resources/rdf.nt"//args(0)//"src/main/resources/rdf.nt"
    val rdf_stats_file = new File(input).getName
    val output = "src/main/resources/rdfstats"//args(1)
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
    println("|        RDF Statistic example       |")
    println("======================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("RDF Dataset Statistics example (" + rdf_stats_file + ")")
      .getOrCreate()

    val triples = NTripleReader.load(sparkSession, URI.create(input))

    // compute  criterias
    val rdf_statistics = RDFStatistics(triples, sparkSession)
    val stats = rdf_statistics.run()
    rdf_statistics.voidify(stats, rdf_stats_file, output)

    sparkSession.stop

  }
}