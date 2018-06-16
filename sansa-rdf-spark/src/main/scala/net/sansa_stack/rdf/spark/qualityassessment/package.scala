
package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.spark.qualityassessment.metrics.availability.DereferenceableUris
import net.sansa_stack.rdf.spark.qualityassessment.metrics.completeness._
import org.apache.jena.graph.{ Triple, Node }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import net.sansa_stack.rdf.spark.qualityassessment.metrics.syntacticvalidity.LiteralNumericRangeChecker._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.syntacticvalidity.XSDDatatypeCompatibleLiterals._

import net.sansa_stack.rdf.spark.qualityassessment.metrics.relevancy.CoverageDetail._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.relevancy.CoverageScope._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.relevancy.AmountOfTriples._

import net.sansa_stack.rdf.spark.qualityassessment.metrics.performance.NoHashURIs._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.understandability.LabeledResources._

import net.sansa_stack.rdf.spark.qualityassessment.metrics.interlinking.ExternalSameAsLinks._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.licensing.HumanReadableLicense._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.licensing.MachineReadableLicense._

import net.sansa_stack.rdf.spark.qualityassessment.metrics.conciseness.ExtensionalConciseness._

/**
 * @author Gezim Sejdiu
 */
package object qualityassessment {

  implicit class QualityAssessmentReader(triples: RDD[Triple]) {

    /**
     * This metric calculates the number of valid redirects of URI.
     * It computes the ratio between the number of all valid redirects
     * (subject + predicates + objects)a.k.a dereferencedURIS and
     * the total number of URIs on the dataset.
     */
    def assessDereferenceableUris() =
      DereferenceableUris.assessDereferenceableUris(triples)

    /**
     * This metric measures the extent to which a resource includes
     * all triples from the dataset that have the resource's URI as the object.
     * The ratio computed is the number of objects that are "back-links"
     * (are part of the resource's URI) and the total number of objects.
     */
    def assessDereferenceableBackLinks() =
      DereferenceableUris.assessDereferenceableBackLinks(triples)

    /**
     * This metric measures the extent to which a resource includes
     * all triples from the dataset that have the resource's URI as the subject.
     * The ratio computed is the number of subjects that are "forward-links"
     * (are part of the resource's URI) and the total number of subjects.
     */
    def assessDereferenceableForwardLinks() =
      DereferenceableUris.assessDereferenceableForwardLinks(triples)

    /**
     * This metric measures the interlinking completeness. Since any resource of a
     * dataset can be interlinked with another resource of a foreign dataset this
     * metric makes a statement about the ratio of interlinked resources to
     * resources that could potentially be interlinked.
     */
    def assessInterlinkingCompleteness() =
      InterlinkingCompleteness.assessInterlinkingCompleteness(triples)

    /**
     * This metric measures the property completeness by checking
     * the missing object values for the given predicate and given class of subjects.
     * A user specifies the RDF class and the RDF predicate, then it checks for each pair
     * whether instances of the given RDF class contain the specified RDF predicate.
     */
    def assessPropertyCompleteness() =
      PropertyCompleteness.assessPropertyCompleteness(triples)

    /**
     * This metric measures the ratio of the number of classes and relations
     * of the gold standard existing in g, and the number of classes and
     * relations in the gold standard.
     */
    def assessSchemaCompleteness() =
      SchemaCompleteness.assessSchemaCompleteness(triples)

  }

  implicit def AssessLiteralNumericRangeCheckerFunctions(dataset: RDD[Triple]) = new LiteralNumericRangeCheckerFunctions(dataset)

  implicit def AssessXSDDatatypeCompatibleLiteralsFunctions(dataset: RDD[Triple]) = new XSDDatatypeCompatibleLiteralsFunctions(dataset)

  implicit def AssessCoverageDetailFunctions(dataset: RDD[Triple]) = new CoverageDetailFunctions(dataset)
  implicit def AssessCoverageScopeFunctions(dataset: RDD[Triple]) = new CoverageScopeFunctions(dataset)
  implicit def AssessAmountOfTriplesFunctions(dataset: RDD[Triple]) = new AmountOfTriplesFunctions(dataset)

  implicit def AssessNoHashURIsFunctions(dataset: RDD[Triple]) = new NoHashURIsFunctions(dataset)

  implicit def AssessLabeledResourcesFunctions(dataset: RDD[Triple]) = new LabeledResourcesFunctions(dataset)

  implicit def AssessExternalSameAsLinksFunctions(dataset: RDD[Triple]) = new ExternalSameAsLinksFunctions(dataset)

  implicit def AssessHumanReadableLicenseFunctions(dataset: RDD[Triple]) = new HumanReadableLicenseFunctions(dataset)
  implicit def AssessMachineReadableLicenseFunctions(dataset: RDD[Triple]) = new MachineReadableLicenseFunctions(dataset)

  implicit def AssessExtensionalConcisenessFunctions(dataset: RDD[Triple]) = new ExtensionalConcisenessFunctions(dataset)

}