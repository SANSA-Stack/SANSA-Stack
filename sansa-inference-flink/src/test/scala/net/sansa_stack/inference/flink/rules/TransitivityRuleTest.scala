package net.sansa_stack.inference.flink.rules

import net.sansa_stack.inference.flink.data.RDFGraphWriter
import net.sansa_stack.inference.flink.forwardchaining.ForwardRuleReasonerRDFS
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.jena.vocabulary.RDFS
import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.flink.data.{RDFGraph, RDFGraphWriter}

import scala.collection.mutable

/**
  * A forward chaining implementation of the RDFS entailment regime.
  *
  * @author Lorenz Buehmann
  */
object TransitivityRuleTest {

  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    // generate graph
    val triples = new mutable.HashSet[RDFTriple]()
    val ns = "http://ex.org/"
    val p1 = RDFS.subClassOf.getURI

    val scale = 1
    val begin = 1
    val end = 10 * scale

    for(i <- begin to end) {
      triples += RDFTriple(ns + "x" + i, p1, ns + "y" + i)
      triples += RDFTriple(ns + "y" + i, p1, ns + "z" + i)
      triples += RDFTriple(ns + "z" + i, p1, ns + "w" + i)
    }

    // graph is a path of length n
    // (x1, p, x2), (x2, p, x3), ..., (x(n-1), p, xn)
    val n = 10
    for (i <- 1 to end) {
      triples += RDFTriple(ns + "x" + i, p1, ns + "x" + (i + 1))
    }

    val triplesDataset = env.fromCollection(triples)

    val graph = RDFGraph(triplesDataset)

    // create reasoner
    val reasoner = new ForwardRuleReasonerRDFS(env)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    inferredGraph.triples.print()

    // write triples to disk
    RDFGraphWriter.writeToFile(inferredGraph, "file:///tmp/flink/tc-test")



  }

}
