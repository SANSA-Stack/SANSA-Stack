package net.sansa_stack.rdf.spark.utils

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.jena.graph.{ Node, NodeFactory, Triple }
import org.scalatest.funsuite.AnyFunSuite

class NodeUtilsTests extends AnyFunSuite with DataFrameSuiteBase {

  test("getting the value of the node should match") {

    val triple = Triple.create(
      NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant"),
      NodeFactory.createURI("http://xmlns.com/foaf/0.1/givenName"),
      NodeFactory.createLiteral("Guy De"))

    val subject = triple.getSubject.toString()

    val nodeValue = NodeUtils.getNodeValue(triple.getSubject)

    assert(nodeValue == subject)
  }

}
