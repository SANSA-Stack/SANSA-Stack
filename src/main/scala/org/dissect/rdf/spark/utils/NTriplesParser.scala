package org.dissect.rdf.spark.utils

import org.apache.jena.riot.RiotReader
import org.apache.jena.riot.Lang
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.graph.Node
import java.io.ByteArrayInputStream

object NTriplesParser {

  // regex pattern for end of triple
  def tripleEndingPattern() = """\s*\.\s*$""".r

  // regex pattern for language tag
  def languageTagPattern() = "@[\\w-]+".r

  /*
   * Parse Triples
   */
  def parseTriple(fn: String): (String, String, String) = {
    val triples = RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(fn.getBytes), Lang.NTRIPLES, "http://example/base").next
    (triples.getSubject.toString(), triples.getPredicate.toString(), if (triples.getObject.isLiteral()) triples.getObject.getLiteralLexicalForm().toString() else triples.getObject().toString())
  }

  def parseTripleAsNode(fn: String): (Node, Node, Node) = {
    val triples = RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(fn.getBytes), Lang.NTRIPLES, "http://example/base").next
    (triples.getSubject(), triples.getPredicate(), triples.getObject())
  }

}
