package net.sansa_stack.rdf.common.partition.layout

import net.sansa_stack.rdf.common.partition.layout.TripleLayoutDouble._
import net.sansa_stack.rdf.common.partition.schema.SchemaStringDouble
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{ Node, NodeFactory, Triple }
import org.scalatest.funsuite.AnyFunSuite


/**
 * @author Gezim Sejdiu
 */
class TripleLayoutDoubleTests extends AnyFunSuite {

  val triple = Triple.create(
    NodeFactory.createURI("http://dbpedia.org/resource/Germany"),
    NodeFactory.createURI("http://dbpedia.org/ontology/populationTotal"),
    NodeFactory.createLiteral("82175700", XSDDatatype.XSDdouble))

  test("getting layout from triple should match") {
    val expectedLayout = new SchemaStringDouble(triple.getSubject.getURI, triple.getObject.getLiteralValue.toString().toDouble)
    assert(fromTriple(triple).equals(expectedLayout))
  }

}
