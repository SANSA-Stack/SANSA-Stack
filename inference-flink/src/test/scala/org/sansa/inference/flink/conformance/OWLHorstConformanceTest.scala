package org.sansa.inference.flink.conformance

import org.apache.flink.api.scala._
import org.apache.jena.rdf.model.Model
import org.sansa.inference.data.RDFTriple
import org.sansa.inference.flink.data.{RDFGraph, RDFGraphWriter}
import org.sansa.inference.test.conformance.OWLHorstConformanceTestBase

import scala.collection.mutable

/**
  * The class is to test the conformance of each materialization rule of OWL Horst entailment.
  *
  * @author Lorenz Buehmann
  *
  */
class OWLHorstConformanceTest extends OWLHorstConformanceTestBase with SharedOWLHorstReasonerContext{

  override def computeInferredModel(triples: mutable.HashSet[RDFTriple]): Model = {
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
