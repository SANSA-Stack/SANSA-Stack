package net.sansa_stack.inference.spark.conformance

import java.io.File

import net.sansa_stack.test.conformance.RDFSConformanceTestBase
import org.apache.jena.rdf.model.Model
import net.sansa_stack.inference.data.{JenaOps}

import scala.collection.mutable
import net.sansa_stack.inference.spark.data.model.RDFGraph
import net.sansa_stack.inference.spark.data.writer.RDFGraphWriter

import org.apache.jena.graph.{Node, NodeFactory, Triple}

/**
  * The class is to test the conformance of each materialization rule of RDFS(simple) entailment.
  *
  * @author Lorenz Buehmann
  *
  */
class RDFSConformanceTest extends RDFSConformanceTestBase(rdfOps = new JenaOps) with SharedRDFSReasonerContext{

  override def computeInferredModel(triples: mutable.HashSet[Triple]): Model = {
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
