package net.sansa_stack.rdf.spark.qualityassessment.metrics.syntacticvalidity

import org.apache.spark.sql.SparkSession
import org.apache.jena.graph.{ Triple, Node }
import org.apache.spark.rdd.RDD

/**
 * Check if the value of a typed literal is valid with regards to
 * the given xsd datatype.
 *
 */
object XSDDatatypeCompatibleLiterals {

  @transient var spark: SparkSession = _

  def apply(dataset: RDD[Triple]) = {

    /*
   * isLiteral(?o)&&getDatatype(?o) && isLexicalFormCompatibleWithDatatype(?o)
   */

    val noMalformedDatatypeLiterals = dataset.filter(f => f.getObject.isLiteral() && isLexicalFormCompatibleWithDatatype(f.getObject))

    noMalformedDatatypeLiterals.map(_.getObject).distinct().count()

  }

  def isLexicalFormCompatibleWithDatatype(node: Node) = node.getLiteralDatatype().isValid(node.getLiteralLexicalForm)

}