package net.sansa_stack.examples.flink.inference

import java.io.File

import scala.collection.mutable
import org.apache.flink.api.scala.ExecutionEnvironment
import net.sansa_stack.inference.rules.ReasoningProfile._
import net.sansa_stack.inference.flink.data.RDFGraphLoader
import net.sansa_stack.inference.flink.forwardchaining.{ForwardRuleReasonerOWLHorst, ForwardRuleReasonerRDFS}
import net.sansa_stack.inference.rules.ReasoningProfile
import net.sansa_stack.inference.flink.data.RDFGraphWriter

object RDFGraphInference {

 def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        "Usage: RDFGraphInference <input> <output> <reasoner")
      System.err.println("Supported 'reasoner' as follows:")
      System.err.println("  rdfs                  Forward Rule Reasoner RDFS")
      System.err.println("  owl-horst             Forward Rule Reasoner OWL Horst")
      System.err.println("  owl-el                Forward Rule Reasoner OWL EL")
      System.err.println("  owl-rl                Forward Rule Reasoner OWL RL")
      System.exit(1)
    }
    val input = args(0)
    val output = args(1)
    val argprofile = args(2)

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

    val env = ExecutionEnvironment.getExecutionEnvironment

    // load triples from disk
    val graph = RDFGraphLoader.loadFromFile(new File(input).getAbsolutePath, env)
    println(s"|G|=${graph.size()}")

    // create reasoner
    val reasoner = profile match {
      case RDFS => new ForwardRuleReasonerRDFS(env)
      case OWL_HORST => new ForwardRuleReasonerOWLHorst(env)
    }

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)
    println(s"|G_inferred|=${inferredGraph.size()}")

    // write triples to disk
    RDFGraphWriter.writeToFile(inferredGraph, new File(output).getAbsolutePath)

    env.execute(s"RDF Graph Inference ($profile)")

  }
}