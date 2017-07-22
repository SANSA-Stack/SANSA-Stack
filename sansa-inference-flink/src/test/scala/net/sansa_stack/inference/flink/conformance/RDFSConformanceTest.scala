package net.sansa_stack.inference.flink.conformance

import net.sansa_stack.inference.flink.data.RDFGraphWriter
import net.sansa_stack.test.conformance.{IntegrationTestSuite, RDFSConformanceTestBase}
import org.apache.jena.rdf.model.Model
import net.sansa_stack.inference.data.{RDFTriple, SimpleRDFOps}
import net.sansa_stack.inference.flink.data.RDFGraph
import org.apache.flink.api.scala._
import org.scalatest.Ignore

import scala.collection.mutable

/**
  * The class is to test the conformance of each materialization rule of RDFS(simple) entailment.
  *
  * @author Lorenz Buehmann
  *
  */
@IntegrationTestSuite
class RDFSConformanceTest extends RDFSConformanceTestBase(rdfOps = new SimpleRDFOps) with SharedRDFSReasonerContext{

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
