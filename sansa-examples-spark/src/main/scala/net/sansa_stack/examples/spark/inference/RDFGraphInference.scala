package net.sansa_stack.examples.spark.inference

import java.io.File
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.model.JenaSparkRDDOps
import net.sansa_stack.inference.spark.RDFGraphMaterializer
import net.sansa_stack.inference.spark.data.RDFGraphLoader
import net.sansa_stack.inference.spark.forwardchaining.ForwardRuleReasonerRDFS
import net.sansa_stack.inference.rules.ReasoningProfile
import net.sansa_stack.inference.spark.forwardchaining.ForwardRuleReasonerOWLHorst
import net.sansa_stack.inference.rules.ReasoningProfile._
import net.sansa_stack.inference.spark.data.RDFGraphWriter

object RDFGraphInference {

  def main(args: Array[String]) = {
    if (args.length < 3) {
      System.err.println(
        "Usage: RDF Graph Inference <input> <output> <reasoner")
      System.err.println("Supported 'reasoner' as follows:")
      System.err.println("  rdfs                  Forward Rule Reasoner RDFS")
      System.err.println("  owl-horst             Forward Rule Reasoner OWL Horst")
      System.err.println("  owl-el                Forward Rule Reasoner OWL EL")
      System.err.println("  owl-rl                Forward Rule Reasoner OWL RL")
      System.exit(1)
    }
    val input = args(0) //"src/main/resources/rdf.nt"
    val output = args(1) //"src/main/resources/res/"
    val argprofile = args(2) //"rdfs"

    val profile = argprofile match {
      case "rdfs"      => ReasoningProfile.RDFS
      case "owl-horst" => ReasoningProfile.OWL_HORST
      case "owl-el"    => ReasoningProfile.OWL_EL
      case "owl-rl"    => ReasoningProfile.OWL_RL

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
      .appName("RDF Graph Inference (" + input + ")")
      .getOrCreate()

    // load triples from disk
    val graph = RDFGraphLoader.loadFromFile(new File(input).getAbsolutePath, sparkSession.sparkContext, 4)

    // create reasoner
    val reasoner = profile match {
      case RDFS      => new ForwardRuleReasonerRDFS(sparkSession.sparkContext)
      case OWL_HORST => new ForwardRuleReasonerOWLHorst(sparkSession.sparkContext)
    }
    println(reasoner)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)
    print(inferredGraph.size())

    // write triples to disk
    RDFGraphWriter.writeToFile(inferredGraph, new File(output).getAbsolutePath)

    //triplesRDD.saveAsTextFile(output)

    sparkSession.stop

  }
}