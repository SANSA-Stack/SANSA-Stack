package net.sansa_stack.rdf.common.partition.core

import java.io.ByteArrayInputStream

import org.aksw.obda.jena.r2rml.impl.R2rmlExporter

import net.sansa_stack.rdf.common.partition.core.RdfPartitionerDefault._
import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils
import net.sansa_stack.rdf.common.partition.schema.{SchemaStringString, SchemaStringStringLang}
import org.aksw.r2rml.jena.vocab.RR
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}
import org.scalatest.funsuite.AnyFunSuite


/**
 * @author Gezim Sejdiu
 */
class RdfPartitionerDefaultTests extends AnyFunSuite {

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

  test("checking if data type is PlainLiteral should match") {
    assert(isPlainLiteralDatatype(triple.getObject.getLiteralDatatypeURI))
  }

  test("checking if [[Node]] is TypedLiteral should match") {
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
        |[ rr:logicalTable        [ rr:tableName  "\"http://xmlns.com/foaf/0.1/givenName_XMLSchema#string_lang\"" ] ;
        |  rr:predicateObjectMap  [ rr:objectMap  [ rr:column      "\"o\"" ;
        |                                           rr:languageColumn  "\"l\""
        |                                         ] ;
        |                           rr:predicate  <http://xmlns.com/foaf/0.1/givenName>
        |                         ] ;
        |  rr:subjectMap          [ rr:column    "\"s\"" ;
        |                           rr:termType  rr:IRI
        |                         ]
        |] .
        |""".stripMargin.getBytes()), Lang.TURTLE)

    val partitionState = RdfPartitionStateDefault(1, "http://xmlns.com/foaf/0.1/givenName",
      2, "http://www.w3.org/2001/XMLSchema#string", true, Set("en", "de", "fr"))

    val model = ModelFactory.createDefaultModel()

    val triplesMaps = R2rmlUtils.createR2rmlMappings(RdfPartitionerDefault, partitionState, model, false)

    // RDFDataMgr.write(System.out, model, RDFFormat.TURTLE_PRETTY)

    // There must be just a single triples map
    assert(triplesMaps.size == 1)

    val triplesMap = triplesMaps(0)
    val actual = triplesMap.getModel

    assert(expected.isIsomorphicWith(model))
  }

  test("export partition with lang tags exploded as R2RML should result in a separate TriplesMap per language tag") {

    val expected : Model = ModelFactory.createDefaultModel
    RDFDataMgr.read(expected, new ByteArrayInputStream(
      """
        | @base          <http://www.w3.org/ns/r2rml#> .
        |[ <#logicalTable>  [ <#sqlQuery>  "SELECT \"s\", \"o\" FROM \"http://xmlns.com/foaf/0.1/givenName_XMLSchema#string_lang\" WHERE \"l\" = 'fr'" ] ;
        |  <#predicateObjectMap>  [ <#objectMap>  [ <#column>  "\"o\"" ;
        |                                           <#language>  "fr"
        |                                         ] ;
        |                           <#predicate>  <http://xmlns.com/foaf/0.1/givenName>
        |                         ] ;
        |  <#subjectMap>  [ <#column>  "\"s\"" ;
        |                   <#termType>  <#IRI>
        |                 ]
        |] .
        |
        |[ <#logicalTable>  [ <#sqlQuery>  "SELECT \"s\", \"o\" FROM \"http://xmlns.com/foaf/0.1/givenName_XMLSchema#string_lang\" WHERE \"l\" = 'de'" ] ;
        |  <#predicateObjectMap>  [ <#objectMap>  [ <#column>  "\"o\"" ;
        |                                           <#language>  "de"
        |                                         ] ;
        |                           <#predicate>  <http://xmlns.com/foaf/0.1/givenName>
        |                         ] ;
        |  <#subjectMap>  [ <#column>  "\"s\"" ;
        |                   <#termType>  <#IRI>
        |                 ]
        |] .
        |
        |[ <#logicalTable>  [ <#sqlQuery>  "SELECT \"s\", \"o\" FROM \"http://xmlns.com/foaf/0.1/givenName_XMLSchema#string_lang\" WHERE \"l\" = 'en'" ] ;
        |  <#predicateObjectMap>  [ <#objectMap>  [ <#column>  "\"o\"" ;
        |                                           <#language>  "en"
        |                                         ] ;
        |                           <#predicate>  <http://xmlns.com/foaf/0.1/givenName>
        |                         ] ;
        |  <#subjectMap>  [ <#column>  "\"s\"" ;
        |                   <#termType>  <#IRI>
        |                 ]
        |] .
        |""".stripMargin.getBytes()), Lang.TURTLE)

    val languages = Set("en", "de", "fr")

    val partitionState = RdfPartitionStateDefault(1, "http://xmlns.com/foaf/0.1/givenName",
      2, "http://www.w3.org/2001/XMLSchema#string", true, languages)

    val actual = ModelFactory.createDefaultModel()

    val triplesMaps = R2rmlUtils.createR2rmlMappings(RdfPartitionerDefault, partitionState, actual, true)

    // RDFDataMgr.write(System.out, actual, RDFFormat.TURTLE_PRETTY)

    assert(triplesMaps.size == languages.size)

    assert(expected.isIsomorphicWith(actual))
  }

  test("R2RML import/export should work") {

    val partitionState = RdfPartitionStateDefault(1, "http://xmlns.com/foaf/0.1/givenName",
      2, "http://www.w3.org/2001/XMLSchema#string", true, Set("en", "de", "fr"))

    val exportModel = ModelFactory.createDefaultModel()
    val triplesMaps = R2rmlUtils.createR2rmlMappings(RdfPartitionerDefault, partitionState, exportModel, true)

     // val exportModel = exportModel // RdfPartitionImportExport.exportAsR2RML(RdfPartitionerDefault, partitionState, true)
    exportModel.write(System.out, "Turtle", "http://www.w3.org/ns/r2rml#")

    // TODO Add serialization and deserialization

    val importedTriplesMaps = R2rmlUtils.streamTriplesMaps(exportModel).toSeq

    assert(triplesMaps.size == importedTriplesMaps.size)

    assert(importedTriplesMaps.head.getModel.isIsomorphicWith(triplesMaps.head.getModel))
  }

  test("determining Layout should match") {
    val expectedLayout = SchemaStringStringLang("http://dbpedia.org/resource/Guy_de_Maupassant", "Guy De", "")
    assert(determineLayout(fromTriple(triple)).fromTriple(triple).equals(expectedLayout))
  }

  test("determining Layout Datatype should match") {
    val expectedLayoutDatatype = SchemaStringString("http://dbpedia.org/resource/Guy_de_Maupassant", "Guy De")
    assert(determineLayoutDatatype(triple.getObject.getLiteralDatatypeURI).fromTriple(triple).equals(expectedLayoutDatatype))
  }
}
