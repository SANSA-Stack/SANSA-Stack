package net.sansa_stack.rdf.common.partition.layout

import net.sansa_stack.rdf.common.partition.layout.TripleLayoutStringLang._
import net.sansa_stack.rdf.common.partition.schema.SchemaStringStringLang
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{ Node, NodeFactory, Triple }
import org.scalatest.funsuite.AnyFunSuite

/**
 * @author Gezim Sejdiu
 */
class TripleLayoutStringLangTests extends AnyFunSuite {

  val triple = Triple.create(
    NodeFactory.createURI("http://dbpedia.org/resource/Germany"),
    NodeFactory.createURI("http://dbpedia.org/ontology/populationTotal"),
    NodeFactory.createLiteral("82175700", "en")) // , XSDDatatype.XSDdouble))

  test("getting layout from triple should match") {
    val expectedLayout = new SchemaStringStringLang(triple.getSubject.getURI, triple.getObject.getLiteralLexicalForm, triple.getObject.getLiteralLanguage)
    assert(fromTriple(triple).equals(expectedLayout))
  }

}
