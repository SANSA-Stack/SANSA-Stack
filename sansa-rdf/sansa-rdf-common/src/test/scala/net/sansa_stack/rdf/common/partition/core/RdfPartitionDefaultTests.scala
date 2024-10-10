package net.sansa_stack.rdf.common.partition.core

import net.sansa_stack.rdf.common.partition.schema.SchemaStringStringLang
import org.apache.jena.graph.{ Node, NodeFactory, Triple }
import org.scalatest.funsuite.AnyFunSuite


/**
 * @author Gezim Sejdiu
 */
class RdfPartitionDefaultTests extends AnyFunSuite {

  val partitioner = RdfPartitionerDefault

  val triple = Triple.create(
    NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant"),
    NodeFactory.createURI("http://xmlns.com/foaf/0.1/givenName"),
    NodeFactory.createLiteral("Guy De"))

//  val partition = new RdfPartitionDefault(1, "http://xmlns.com/foaf/0.1/givenName",
//    2, "http://www.w3.org/2001/XMLSchema#string", true)

  val partition = partitioner.fromTriple(triple)

  test("getting layout should match") {
    val expectedLayout = new SchemaStringStringLang("http://dbpedia.org/resource/Guy_de_Maupassant", "Guy De", "")
    assert(partitioner.determineLayout(partition).fromTriple(triple).equals(expectedLayout))
  }

  test("getting partition layout should match") {
    // val expectedLayout = partition.matches(triple)
    assert(partitioner.matches(partition, triple).equals(true))
  }

  test("repated partitioning should yield equivalent partitions w.r.t. equals()") {
    val partition2 = partitioner.fromTriple(triple)
    assert(partition == partition2)
  }

}
