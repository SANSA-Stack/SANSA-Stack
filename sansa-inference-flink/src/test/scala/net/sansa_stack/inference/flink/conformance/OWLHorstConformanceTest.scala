package net.sansa_stack.inference.flink.conformance

import net.sansa_stack.inference.flink.data.RDFGraphWriter
import net.sansa_stack.test.conformance.OWLHorstConformanceTestBase
import org.apache.flink.api.scala._
import org.apache.jena.rdf.model.Model
import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.flink.data.{RDFGraph, RDFGraphWriter}

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
