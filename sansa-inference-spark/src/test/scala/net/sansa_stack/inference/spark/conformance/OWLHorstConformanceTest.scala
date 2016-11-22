package net.sansa_stack.inference.spark.conformance

import net.sansa_stack.test.conformance.OWLHorstConformanceTestBase
import org.apache.jena.rdf.model.Model
import org.apache.spark.{SparkConf, SparkContext}
import net.sansa_stack.inference.data.RDFTriple
import net.sansa_stack.inference.spark.data.{RDFGraph, RDFGraphWriter}
import net.sansa_stack.inference.spark.forwardchaining.ForwardRuleReasonerRDFS

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
