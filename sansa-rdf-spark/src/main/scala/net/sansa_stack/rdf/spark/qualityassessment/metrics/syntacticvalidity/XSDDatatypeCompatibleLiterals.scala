package net.sansa_stack.rdf.spark.qualityassessment.metrics.syntacticvalidity

import org.apache.spark.sql.SparkSession
import org.apache.jena.graph.{ Triple, Node }
import org.apache.spark.rdd.RDD
import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._

/**
 * @author Gezim Sejdiu
 */
object XSDDatatypeCompatibleLiterals {
  implicit class XSDDatatypeCompatibleLiteralsFunctions(dataset: RDD[Triple]) extends Serializable {

    /**
     * Check if the value of a typed literal is valid with regards to
     * the given xsd datatype.
     */
    def assessXSDDatatypeCompatibleLiterals() = {

      /*
   * isLiteral(?o)&&getDatatype(?o) && isLexicalFormCompatibleWithDatatype(?o)
   */
      val noMalformedDatatypeLiterals = dataset.filter(f => f.getObject.isLiteral() && isLexicalFormCompatibleWithDatatype(f.getObject))
      
      noMalformedDatatypeLiterals.map(_.getObject).distinct().count()

    }
  }
}