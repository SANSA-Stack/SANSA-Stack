package net.sansa_stack.inference.flink.conformance

import scala.collection.mutable

import org.apache.flink.api.scala._
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.Model

import net.sansa_stack.inference.data.{Jena, JenaOps}
import net.sansa_stack.inference.flink.data.{RDFGraph, RDFGraphWriter}
import net.sansa_stack.test.conformance.OWLHorstConformanceTestBase

/**
  * The class is to test the conformance of each materialization rule of OWL Horst entailment.
  *
  * @author Lorenz Buehmann
  *
  */
class OWLHorstConformanceTest
  extends OWLHorstConformanceTestBase[Jena](rdfOps = new JenaOps)
    with SharedOWLHorstReasonerContext{

  override def computeInferredModel(triples: mutable.HashSet[Triple]): Model = {
    // distribute triples
    val triplesRDD = env.fromCollection(triples)

    // create graph
    val graph = RDFGraph(triplesRDD)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    inferredGraph.triples.print()

    // convert to JENA model
    val inferredModel = RDFGraphWriter.convertToModel(inferredGraph)

    inferredModel
  }
}
