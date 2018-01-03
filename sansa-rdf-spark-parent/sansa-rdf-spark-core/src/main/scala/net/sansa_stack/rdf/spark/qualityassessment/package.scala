
package net.sansa_stack.rdf.spark

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.spark.qualityassessment.metrics.completeness.InterlinkingCompleteness._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.completeness.PropertyCompleteness._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.completeness.SchemaCompleteness._

import net.sansa_stack.rdf.spark.qualityassessment.metrics.syntacticvalidity.LiteralNumericRangeChecker._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.syntacticvalidity.XSDDatatypeCompatibleLiterals._

package object qualityassessment {

  implicit def AssessInterlinkingCompletenessFunctions(dataset: RDD[Triple]) = new InterlinkingCompletenessFunctions(dataset)

  implicit def AssessPropertyCompletenessFunctions(dataset: RDD[Triple]) = new PropertyCompletenessFunctions(dataset)

  implicit def AssessSchemaCompletenessFunctions(dataset: RDD[Triple]) = new SchemaCompletenessFunctions(dataset)

  implicit def AssessLiteralNumericRangeCheckerFunctions(dataset: RDD[Triple]) = new LiteralNumericRangeCheckerFunctions(dataset)

  implicit def AssessXSDDatatypeCompatibleLiteralsFunctions(dataset: RDD[Triple]) = new XSDDatatypeCompatibleLiteralsFunctions(dataset)

}