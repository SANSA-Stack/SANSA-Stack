
package net.sansa_stack.rdf.spark

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.spark.qualityassessment.metrics.completeness.InterlinkingCompleteness._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.completeness.PropertyCompleteness._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.completeness.SchemaCompleteness._

import net.sansa_stack.rdf.spark.qualityassessment.metrics.syntacticvalidity.LiteralNumericRangeChecker._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.syntacticvalidity.XSDDatatypeCompatibleLiterals._

import net.sansa_stack.rdf.spark.qualityassessment.metrics.availability.DereferenceableUris._

import net.sansa_stack.rdf.spark.qualityassessment.metrics.relevancy.CoverageDetail._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.relevancy.CoverageScope._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.relevancy.AmountOfTriples._

import net.sansa_stack.rdf.spark.qualityassessment.metrics.performance.NoHashURIs._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.understandability.LabeledResources._

import net.sansa_stack.rdf.spark.qualityassessment.metrics.interlinking.ExternalSameAsLinks._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.licensing.HumanReadableLicense._

package object qualityassessment {

  implicit def AssessInterlinkingCompletenessFunctions(dataset: RDD[Triple]) = new InterlinkingCompletenessFunctions(dataset)

  implicit def AssessPropertyCompletenessFunctions(dataset: RDD[Triple]) = new PropertyCompletenessFunctions(dataset)

  implicit def AssessSchemaCompletenessFunctions(dataset: RDD[Triple]) = new SchemaCompletenessFunctions(dataset)

  implicit def AssessLiteralNumericRangeCheckerFunctions(dataset: RDD[Triple]) = new LiteralNumericRangeCheckerFunctions(dataset)

  implicit def AssessXSDDatatypeCompatibleLiteralsFunctions(dataset: RDD[Triple]) = new XSDDatatypeCompatibleLiteralsFunctions(dataset)

  implicit def AssessDereferenceableUrisFunctions(dataset: RDD[Triple]) = new DereferenceableUrisFunctions(dataset)

  implicit def AssessCoverageDetailFunctions(dataset: RDD[Triple]) = new CoverageDetailFunctions(dataset)
  implicit def AssessCoverageScopeFunctions(dataset: RDD[Triple]) = new CoverageScopeFunctions(dataset)
  implicit def AssessAmountOfTriplesFunctions(dataset: RDD[Triple]) = new AmountOfTriplesFunctions(dataset)

  implicit def AssessNoHashURIsFunctions(dataset: RDD[Triple]) = new NoHashURIsFunctions(dataset)

  implicit def AssessLabeledResourcesFunctions(dataset: RDD[Triple]) = new LabeledResourcesFunctions(dataset)

  implicit def AssessExternalSameAsLinksFunctions(dataset: RDD[Triple]) = new ExternalSameAsLinksFunctions(dataset)

  implicit def AssessHumanReadableLicenseFunctions(dataset: RDD[Triple]) = new HumanReadableLicenseFunctions(dataset)

}