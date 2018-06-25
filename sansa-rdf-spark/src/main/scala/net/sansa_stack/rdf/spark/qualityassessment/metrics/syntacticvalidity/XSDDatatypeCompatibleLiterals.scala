package net.sansa_stack.rdf.spark.qualityassessment.metrics.syntacticvalidity

import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.rdd.RDD

/**
 * @author Gezim Sejdiu
 */
object XSDDatatypeCompatibleLiterals {

  /**
   * Check if the value of a typed literal is valid with regards to
   * the given xsd datatype.
   */
  def assessXSDDatatypeCompatibleLiterals(dataset: RDD[Triple]): Long = {

    /**
     * isLiteral(?o)&&getDatatype(?o) && isLexicalFormCompatibleWithDatatype(?o)
     */
    val noMalformedDatatypeLiterals = dataset.filter(f =>
      f.getObject.isLiteral() && isLexicalFormCompatibleWithDatatype(f.getObject))

    // val metricValue = noMalformedDatatypeLiterals.map(_.getObject).distinct().count()
    val metricValue = noMalformedDatatypeLiterals.distinct().count()
    metricValue
  }
}
