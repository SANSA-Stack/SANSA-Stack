package org.dissect.rdf.spark.utils

import org.apache.jena.riot.RiotReader
import org.dissect.rdf.spark.model.StringInputStream
import org.apache.jena.riot.Lang

object NTriplesParser {

  // regex pattern for end of triple
  def tripleEndingPattern() = """\s*\.\s*$""".r

  // regex pattern for language tag
  def languageTagPattern() = "@[\\w-]+".r

  /*  private def parseSubject(c: Char): Unit = {
    c match {
      case '<' => parseURI()
      case '_' => parseBNode()
      case x => throw Error("Subject of Triple must start with a URI or bnode .")
    }
  }
  
  private def parseObject(c: Int) = {
    c match {
      case '<' => URI()
      case '"' => Literal()
      case '_' => BlankNode()
      case other => throw Error("illegal character to start triple entity ( subject, relation, or object)")
    }
  }*/

  /*
   * Parse Triples
   */
  def parseTriple(fn: String) = {
    val triples = RiotReader.createIteratorTriples(new StringInputStream(fn), Lang.NTRIPLES, "http://example/base").next
    (triples.getSubject.toString(), triples.getPredicate.toString(), if (triples.getObject.isLiteral()) triples.getObject.getLiteralLexicalForm().toString() else triples.getObject().toString())
  }

}