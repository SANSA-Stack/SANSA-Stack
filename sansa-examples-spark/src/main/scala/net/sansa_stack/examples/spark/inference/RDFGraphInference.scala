package net.sansa_stack.examples.spark.inference

import java.io.File
import java.net.URI

import net.sansa_stack.inference.rules.{ RDFSLevel, ReasoningProfile }
import net.sansa_stack.inference.rules.ReasoningProfile._
import net.sansa_stack.inference.spark.data.loader.RDFGraphLoader
import net.sansa_stack.inference.spark.data.writer.RDFGraphWriter
import net.sansa_stack.inference.spark.forwardchaining.{ ForwardRuleReasonerOWLHorst, ForwardRuleReasonerRDFS, ForwardRuleReasonerRDFSDataset, TransitiveReasoner }
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object RDFGraphInference {

  def main(args: Array[String]) = {
    if (args.length < 3) {
      System.err.println(
        "Usage: RDFGraphInference <input> <output> <reasoner")
      System.err.println("Supported 'reasoner' as follows:")
      System.err.println("  rdfs                  Forward Rule Reasoner RDFS (Full)")
      System.err.println("  rdfs-simple           Forward Rule Reasoner RDFS (Simple)")
      System.err.println("  owl-horst             Forward Rule Reasoner OWL Horst")
      System.err.println("  transitive            Forward Rule Transitive Reasoner")
      System.exit(1)
    }
    val input = args(0) //"src/main/resources/rdf.nt"
    val output = args(1) //"src/main/resources/res/"
    val argprofile = args(2) //"rdfs"

    val profile = argprofile match {
      case "rdfs"        => ReasoningProfile.RDFS
      case "rdfs-simple" => ReasoningProfile.RDFS_SIMPLE
      case "owl-horst"   => ReasoningProfile.OWL_HORST
      case "transitive"  => ReasoningProfile.TRANSITIVE

    }
    val optionsList = args.drop(3).map { arg =>
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
    println("|        RDF Graph Inference         |")
    println("======================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.hadoop.validateOutputSpecs", "false") //override output files
      .config("spark.default.parallelism", "4")
      .appName(s"RDF Graph Inference ($profile)")
      .getOrCreate()

    // the degree of parallelism
    val parallelism = 4

    // load triples from disk
    val graph = RDFGraphLoader.loadFromDisk(sparkSession, URI.create(input), parallelism)
    println(s"|G|=${graph.size()}")

    // create reasoner
    val reasoner = profile match {
      case TRANSITIVE => new TransitiveReasoner(sparkSession.sparkContext, parallelism)
      case RDFS       => new ForwardRuleReasonerRDFS(sparkSession.sparkContext, parallelism)
      case RDFS_SIMPLE =>
        var r = new ForwardRuleReasonerRDFS(sparkSession.sparkContext, parallelism) //.level.+(RDFSLevel.SIMPLE)
        r.level = RDFSLevel.SIMPLE
        r
      case OWL_HORST => new ForwardRuleReasonerOWLHorst(sparkSession.sparkContext)
    }

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)
    println(s"|G_inferred|=${inferredGraph.size()}")

    // write triples to disk
    RDFGraphWriter.writeToDisk(inferredGraph, output)

    sparkSession.stop
  }
}