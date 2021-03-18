package net.sansa_stack.inference.flink.conformance

import scala.collection.mutable

import org.apache.flink.api.scala._
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.Model

import net.sansa_stack.inference.data.{Jena, JenaOps}
import net.sansa_stack.inference.flink.data.{RDFGraph, RDFGraphWriter}
import net.sansa_stack.inference.test.conformance.RDFSConformanceTestBase

/**
  * The class is used to check the conformance of each materialization rule of RDFS(simple) entailment.
  *
  * @author Lorenz Buehmann
  *
  */
class RDFSConformanceTest
  extends RDFSConformanceTestBase[Jena](rdfOps = new JenaOps)
    with SharedRDFSReasonerContext{

  override def computeInferredModel(triples: mutable.HashSet[Triple]): Model = {
    // distribute triples
    val triplesDS = env.fromCollection(triples)

    // create graph
    val graph = RDFGraph(triplesDS)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    // convert to Jena model
    val inferredModel = RDFGraphWriter.convertToModel(inferredGraph)

    inferredModel
  }
}
