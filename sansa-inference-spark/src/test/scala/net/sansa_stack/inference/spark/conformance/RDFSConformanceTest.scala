package net.sansa_stack.inference.spark.conformance

import net.sansa_stack.test.conformance.RDFSConformanceTestBase
import org.apache.jena.rdf.model.Model

import net.sansa_stack.inference.data.RDFTriple
import scala.collection.mutable

import net.sansa_stack.inference.spark.data.model.RDFGraph
import net.sansa_stack.inference.spark.data.writer.RDFGraphWriter

/**
  * The class is to test the conformance of each materialization rule of RDFS(simple) entailment.
  *
  * @author Lorenz Buehmann
  *
  */
class RDFSConformanceTest extends RDFSConformanceTestBase with SharedRDFSReasonerContext{

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
