package net.sansa_stack.rdf.flink.qualityassessment.metrics.syntacticvalidity

import org.apache.jena.graph.{ Triple, Node }
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import net.sansa_stack.rdf.flink.data.RDFGraph

/**
 * Check if the value of a typed literal is valid with regards to
 * the given xsd datatype.
 *
 */
object XSDDatatypeCompatibleLiterals {

  @transient var env: ExecutionEnvironment = _

  def apply(rdfgraph: RDFGraph) = {

    /*
   * isLiteral(?o)&&getDatatype(?o) && isLexicalFormCompatibleWithDatatype(?o)
   */
    val dataset = rdfgraph.triples

    val noMalformedDatatypeLiterals = dataset.filter(f => f.getObject.isLiteral() && isLexicalFormCompatibleWithDatatype(f.getObject))

    noMalformedDatatypeLiterals.map(_.getObject).distinct().count()

  }

  def isLexicalFormCompatibleWithDatatype(node: Node) = node.getLiteralDatatype().isValid(node.getLiteralLexicalForm)

}