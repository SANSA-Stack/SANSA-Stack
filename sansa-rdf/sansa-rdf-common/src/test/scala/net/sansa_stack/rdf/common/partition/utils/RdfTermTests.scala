package net.sansa_stack.rdf.common.partition.utils

import net.sansa_stack.rdf.common.partition.utils.RdfTerm._
import org.apache.jena.graph.{ Node, NodeFactory, Triple }
import org.scalatest.funsuite.AnyFunSuite

/**
 * @author Gezim Sejdiu
 */
class RdfTermTests extends AnyFunSuite {

  val triple = Triple.create(
    NodeFactory.createURI("http://dbpedia.org/resource/Germany"),
    NodeFactory.createURI("http://dbpedia.org/ontology/populationTotal"),
    NodeFactory.createLiteral("82175700"))

  test("converting rdf term to node should pass") {
    val term = nodeToTerm(triple.getSubject)
    assert(termToNode(term).equals(triple.getSubject))
  }

  test("converting node to rdf term should pass") {
    val requiredRDFTerm = new RdfTerm(1, "http://dbpedia.org/resource/Germany", null, null)
    assert(nodeToTerm(triple.getSubject).equals(requiredRDFTerm))
  }

}
