package org.sansa.inference.spark.conformance

import org.apache.jena.rdf.model.Model
import org.apache.spark.{SparkConf, SparkContext}
import org.sansa.inference.data.RDFTriple
import org.sansa.inference.spark.data.{RDFGraph, RDFGraphWriter}
import org.sansa.inference.spark.forwardchaining.ForwardRuleReasonerRDFS
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
    val triplesRDD = sc.parallelize(triples.toSeq, 2)

    // create graph
    val graph = RDFGraph(triplesRDD)

    // compute inferred graph
    val inferredGraph = reasoner.apply(graph)

    // convert to JENA model
    val inferredModel = RDFGraphWriter.convertToModel(inferredGraph)

    inferredModel
  }
}
