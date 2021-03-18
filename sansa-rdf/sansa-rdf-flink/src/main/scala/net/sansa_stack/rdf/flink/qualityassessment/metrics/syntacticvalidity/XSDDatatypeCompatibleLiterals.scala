package net.sansa_stack.rdf.flink.qualityassessment.metrics.syntacticvalidity

import net.sansa_stack.rdf.common.qualityassessment.utils.NodeUtils._
import org.apache.flink.api.scala._
import org.apache.jena.graph.Triple

/**
  * @author Gezim Sejdiu
  */
object XSDDatatypeCompatibleLiterals {

  /**
    * Check if the value of a typed literal is valid with regards to
    * the given xsd datatype.
    */
  def assessXSDDatatypeCompatibleLiterals(triples: DataSet[Triple]): Long = {

    /**
      * isLiteral(?o)&&getDatatype(?o) && isLexicalFormCompatibleWithDatatype(?o)
      */

    val noMalformedDatatypeLiterals = triples.filter(f => f.getObject.isLiteral() && isLexicalFormCompatibleWithDatatype(f.getObject))

    noMalformedDatatypeLiterals.map(_.getObject).distinct(_.hashCode()).count()

  }
}
