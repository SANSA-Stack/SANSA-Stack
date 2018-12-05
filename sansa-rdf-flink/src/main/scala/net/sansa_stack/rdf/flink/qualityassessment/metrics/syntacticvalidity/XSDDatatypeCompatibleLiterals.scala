package net.sansa_stack.rdf.flink.qualityassessment.metrics.syntacticvalidity

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.jena.graph.{ Node, Triple }

/**
 * Check if the value of a typed literal is valid with regards to
 * the given xsd datatype.
 *
 */
object XSDDatatypeCompatibleLiterals {

  @transient var env: ExecutionEnvironment = _

  def apply(triples: DataSet[Triple]): Long = {

    /**
     * isLiteral(?o)&&getDatatype(?o) && isLexicalFormCompatibleWithDatatype(?o)
     */

    val noMalformedDatatypeLiterals = triples.filter(f => f.getObject.isLiteral() && isLexicalFormCompatibleWithDatatype(f.getObject))

    noMalformedDatatypeLiterals.map(_.getObject).distinct().count()

  }

  def isLexicalFormCompatibleWithDatatype(node: Node): Boolean = node.getLiteralDatatype().isValid(node.getLiteralLexicalForm)

}
