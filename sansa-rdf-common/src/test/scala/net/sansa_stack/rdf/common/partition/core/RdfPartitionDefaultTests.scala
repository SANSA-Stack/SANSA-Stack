package net.sansa_stack.rdf.common.partition.core

import org.scalatest.FunSuite

import net.sansa_stack.rdf.common.partition.schema.SchemaStringStringLang
import org.apache.jena.graph.{ Node, NodeFactory, Triple }

class RdfPartitionDefaultTests extends FunSuite {

  val triple = Triple.create(
    NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant"),
    NodeFactory.createURI("http://xmlns.com/foaf/0.1/givenName"),
    NodeFactory.createLiteral("Guy De"))

  val partition = new RdfPartitionDefault(1, "http://xmlns.com/foaf/0.1/givenName",
    2, "http://www.w3.org/2001/XMLSchema#string", true)

  test("getting layout should match") {
    val expectedLayout = new SchemaStringStringLang("http://dbpedia.org/resource/Guy_de_Maupassant", "Guy De", "")
    assert(partition.layout.fromTriple(triple).equals(expectedLayout))
  }

  test("getting partition layout should match") {
    val expectedLayout = partition.matches(triple)
    assert(partition.matches(triple).equals(expectedLayout))
  }

}
