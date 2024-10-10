package net.sansa_stack.rdf.common.partition.layout

import net.sansa_stack.rdf.common.partition.layout.TripleLayoutString._
import net.sansa_stack.rdf.common.partition.schema.SchemaStringString
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{ Node, NodeFactory, Triple }
import org.scalatest.funsuite.AnyFunSuite

/**
 * @author Gezim Sejdiu
 */
class TripleLayoutStringTests extends AnyFunSuite {

  val triple = Triple.create(
    NodeFactory.createURI("http://dbpedia.org/resource/Germany"),
    NodeFactory.createURI("http://dbpedia.org/ontology/populationTotal"),
    NodeFactory.createLiteral("82175700", XSDDatatype.XSDinteger))

  test("getting layout from triple should match") {
    val expectedLayout = new SchemaStringString(triple.getSubject.getURI, triple.getObject.getLiteralLexicalForm)
    assert(fromTriple(triple).equals(expectedLayout))
  }

}
