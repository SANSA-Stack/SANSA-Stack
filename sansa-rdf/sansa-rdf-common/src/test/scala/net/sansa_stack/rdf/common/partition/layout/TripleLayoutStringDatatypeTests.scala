package net.sansa_stack.rdf.common.partition.layout

import net.sansa_stack.rdf.common.partition.layout.TripleLayoutStringDatatype._
import net.sansa_stack.rdf.common.partition.schema.SchemaStringStringType
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{ Node, NodeFactory, Triple }
import org.scalatest.funsuite.AnyFunSuite

/**
 * @author Gezim Sejdiu
 */
class TripleLayoutStringDatatypeTests extends AnyFunSuite {

  val triple = Triple.create(
    NodeFactory.createURI("http://dbpedia.org/resource/Germany"),
    NodeFactory.createURI("http://dbpedia.org/ontology/populationTotal"),
    NodeFactory.createLiteral("82175700", XSDDatatype.XSDlong))

  test("getting layout from triple should match") {
    val expectedLayout = new SchemaStringStringType(triple.getSubject.getURI, triple.getObject.getLiteralLexicalForm, triple.getObject.getLiteralDatatype.getURI)
    assert(fromTriple(triple).equals(expectedLayout))
  }

}
