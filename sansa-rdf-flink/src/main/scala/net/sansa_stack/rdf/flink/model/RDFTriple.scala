package net.sansa_stack.rdf.flink.model

import org.apache.jena.graph.{ Triple => JTriple, Node => JNode }
/**
 * A data structure for a set of triples.
 *
 * @author Gezim Sejdiu
 *
 */

case class RDFTriple(subject: JNode, predicate: JNode, `object`: JNode) extends JTriple(subject, predicate, `object`) with Serializable {

  def dataType(literal: String): String = {
    val index = literal.indexOf("^^")
    var res = "";
    if (index > -1)
      res = literal.substring(index + 2)
    res
  }

  def languageTag(literal: String): String = {
    val index = literal.indexOf("@")
    var res = "";
    if (index > -1)
      res = literal.substring(index + 1)
    res
  }
}
