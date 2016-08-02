package org.sansa.inference.flink

import org.apache.flink.api.java.utils.ParameterTool
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

    // get params
    val params: ParameterTool = ParameterTool.fromArgs(args)

    if(params.has("input") && params.has("output")) {

      // set up the execution environment
      val env = ExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(4)

      // make parameters available in the web interface
      env.getConfig.setGlobalJobParameters(params)

      // load triples from disk
      val graph = RDFGraphLoader.loadFromFile(params.get("input"), env)

      // create reasoner
      val reasoner = new ForwardRuleReasonerRDFS(env)

      // compute inferred graph
      val inferredGraph = reasoner.apply(graph)

      // write triples to disk
      RDFGraphWriter.writeToFile(inferredGraph, params.get("output"))

      env.execute("RDF graph materialization")

    } else {
      System.err.println("Usage: RDFGraphMaterializer --input <sourceFile> --output <targetFile>")
      System.exit(1)
    }
  }

}
