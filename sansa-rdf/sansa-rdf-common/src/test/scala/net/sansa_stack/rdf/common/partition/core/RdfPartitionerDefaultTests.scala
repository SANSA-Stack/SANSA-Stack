package net.sansa_stack.rdf.common.partition.core

import java.io.ByteArrayInputStream

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault._
import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils
import net.sansa_stack.rdf.common.partition.schema.{SchemaStringString, SchemaStringStringLang}
import org.aksw.r2rml.jena.vocab.RR
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}
import org.scalatest.FunSuite


/**
 * @author Gezim Sejdiu
 */
class RdfPartitionerDefaultTests extends FunSuite {

  val triple = Triple.create(
    NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant"),
    NodeFactory.createURI("http://xmlns.com/foaf/0.1/givenName"),
    NodeFactory.createLiteral("Guy De"))

  test("getting URI or BNode string should match") {
    val node = NodeFactory.createURI("http://dbpedia.org/resource/Guy_de_Maupassant")
    assert(getUriOrBNodeString(triple.getSubject).matches(node.getURI))
  }

  test("getting RDF Term type should match") {
    assert(getRdfTermType(triple.getSubject) == 1)
  }

  test("chechking if data type is PlainLiteral should match") {
    assert(isPlainLiteralDatatype(triple.getObject.getLiteralDatatypeURI))
  }

  test("chechking if [[Node]] is TypedLiteral should match") {
    assert(!isTypedLiteral(triple.getObject))
  }

  test("getting partitioning layout from [[Triple]] should match") {
    val expectedPartition = new RdfPartitionStateDefault(1, "http://xmlns.com/foaf/0.1/givenName",
      2, "http://www.w3.org/2001/XMLSchema#string", true, Set())
    assert(fromTriple(triple).equals(expectedPartition))
  }

  test("export partition as R2RML should work") {

    val expected : Model = ModelFactory.createDefaultModel
    RDFDataMgr.read(expected, new ByteArrayInputStream(
      """
        | @prefix rr:    <http://www.w3.org/ns/r2rml#> .
        |
        |[ rr:logicalTable        [ rr:tableName  "http://xmlns.com/foaf/0.1/givenName_XMLSchema#string_lang" ] ;
        |  rr:predicateObjectMap  [ rr:objectMap  [ rr:column      "o" ;
        |                                           rr:langColumn  "l"
        |                                         ] ;
        |                           rr:predicate  <http://xmlns.com/foaf/0.1/givenName>
        |                         ] ;
        |  rr:subjectMap          [ rr:column    "s" ;
        |                           rr:datatype  rr:IRI
        |                         ]
        |] .
        |""".stripMargin.getBytes()), Lang.TURTLE)

    val partitionState = new RdfPartitionStateDefault(1, "http://xmlns.com/foaf/0.1/givenName",
      2, "http://www.w3.org/2001/XMLSchema#string", true, Option.empty)

    val triplesMaps = R2rmlUtils.createR2rmlMappings(RdfPartitionerDefault, partitionState)

    // There must be just a single triples map
    assert(triplesMaps.size == 1)

    val triplesMap = triplesMaps(0)
    val actual = triplesMap.getModel

    assert(expected.isIsomorphicWith(actual))
  }

  test("determining Layout should match") {
    val expectedLayout = new SchemaStringStringLang("http://dbpedia.org/resource/Guy_de_Maupassant", "Guy De", "")
    assert(determineLayout(fromTriple(triple)).fromTriple(triple).equals(expectedLayout))
  }

  test("determining Layout Datatype should match") {
    val expectedLayoutDatatype = new SchemaStringString("http://dbpedia.org/resource/Guy_de_Maupassant", "Guy De")
    assert(determineLayoutDatatype(triple.getObject.getLiteralDatatypeURI).fromTriple(triple).equals(expectedLayoutDatatype))
  }
}
