package org.sansa.inference.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.sansa.inference.flink.data.{RDFGraphLoader, RDFGraphWriter}
import org.sansa.inference.flink.forwardchaining.ForwardRuleReasonerRDFS

/**
  * The class to compute the RDFS materialization of a given RDF graph.
  *
  * @author Lorenz Buehmann
  *
  */
object RDFGraphMaterializer {


  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: RDFGraphMaterializer <sourceFile> <targetFile>")
      System.exit(1)
    }

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    // load triples from disk
    val graph = RDFGraphLoader.loadFromFile(args(0), env)

    // create reasoner
    val reasoner = new ForwardRuleReasonerRDFS(env)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    // write triples to disk
    RDFGraphWriter.writeToFile(inferredGraph, args(1))

//    env.execute()
  }

}
