
package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.spark.qualityassessment.metrics.availability.DereferenceableUris
import net.sansa_stack.rdf.spark.qualityassessment.metrics.completeness._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.conciseness.ExtensionalConciseness
import net.sansa_stack.rdf.spark.qualityassessment.metrics.interlinking.ExternalSameAsLinks
import net.sansa_stack.rdf.spark.qualityassessment.metrics.licensing._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.performance.NoHashURIs
import net.sansa_stack.rdf.spark.qualityassessment.metrics.relevancy._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.reprconciseness._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.syntacticvalidity._
import net.sansa_stack.rdf.spark.qualityassessment.metrics.understandability.LabeledResources
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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
    def assessDereferenceableUris(): Double =
      DereferenceableUris.assessDereferenceableUris(triples)

    /**
     * This metric measures the extent to which a resource includes
     * all triples from the dataset that have the resource's URI as the object.
     * The ratio computed is the number of objects that are "back-links"
     * (are part of the resource's URI) and the total number of objects.
     */
    def assessDereferenceableBackLinks(): Double =
      DereferenceableUris.assessDereferenceableBackLinks(triples)

    /**
     * This metric measures the extent to which a resource includes
     * all triples from the dataset that have the resource's URI as the subject.
     * The ratio computed is the number of subjects that are "forward-links"
     * (are part of the resource's URI) and the total number of subjects.
     */
    def assessDereferenceableForwardLinks(): Double =
      DereferenceableUris.assessDereferenceableForwardLinks(triples)

    /**
     * This metric measures the interlinking completeness. Since any resource of a
     * dataset can be interlinked with another resource of a foreign dataset this
     * metric makes a statement about the ratio of interlinked resources to
     * resources that could potentially be interlinked.
     */
    def assessInterlinkingCompleteness(): Long =
      InterlinkingCompleteness.assessInterlinkingCompleteness(triples)

    /**
     * This metric measures the property completeness by checking
     * the missing object values for the given predicate and given class of subjects.
     * A user specifies the RDF class and the RDF predicate, then it checks for each pair
     * whether instances of the given RDF class contain the specified RDF predicate.
     */
    def assessPropertyCompleteness(): Long =
      PropertyCompleteness.assessPropertyCompleteness(triples)

    /**
     * This metric measures the ratio of the number of classes and relations
     * of the gold standard existing in g, and the number of classes and
     * relations in the gold standard.
     */
    def assessSchemaCompleteness(): Double =
      SchemaCompleteness.assessSchemaCompleteness(triples)

    /**
     * The extensional conciseness
     * This metric metric checks for redundant resources in the assessed dataset,
     * and thus measures the number of unique instances found in the dataset.
     * @return  No. of unique subjects / Total No. of subjects
     */
    def assessExtensionalConciseness(): Double =
      ExtensionalConciseness.assessExtensionalConciseness(triples)

    /**
     * Checks the sameAs externals links.
     */
    def assessExternalSameAsLinks(): Double =
      ExternalSameAsLinks.assessExternalSameAsLinks(triples)

    /**
     * Human -readable indication of a license
     * This metric checks whether a human-readable text, stating the of licensing model
     * attributed to the resource, has been provided as part of the dataset.
     * It looks for objects containing literal values and analyzes the text
     * searching for key, licensing related terms.
     */
    def assessHumanReadableLicense(): Double =
      HumanReadableLicense.assessHumanReadableLicense(triples)

    /**
     * Machine -readable indication of a license
     * This metric checks whether a machine-readable text, stating the of licensing model
     * attributed to the resource, has been provided as part of the dataset.
     * It looks for objects containing literal values and analyzes the text
     * searching for key, licensing related terms.
     */
    def assessMachineReadableLicense(): Double =
      MachineReadableLicense.assessMachineReadableLicense(triples)

    /**
     * Checks if a URI contains hashs.
     */
    def assessNoHashUris(): Double =
      NoHashURIs.assessNoHashUris(triples)

    /**
     * Computes the size of the triples.
     */
    def assessAmountOfTriples(): Double =
      AmountOfTriples.assessAmountOfTriples(triples)

    /**
     * This metric measures the the coverage (i.e. number of entities described
     * in a dataset) and level of detail (i.e. number of properties) in a dataset
     * to ensure that the data retrieved is appropriate for the task at hand.
     */
    def assessCoverageDetail(): Double =
      CoverageDetail.assessCoverageDetail(triples)

    /**
     * This metric calculate the coverage of a dataset referring to the covered scope.
     * This covered scope is expressed as the number of 'instances' statements are made about.
     */
    def assessCoverageScope(): Double =
      CoverageScope.assessCoverageScope(triples)

    /**
     * This metric calculates the number of non Queryable URIs.
     * It computes the ratio between the number of all non queryable URIs
     * and the total number of URIs on the dataset.
     */
    def assessQueryParamFreeURIs(): Double =
      QueryParamFreeURIs.assessQueryParamFreeURIs(triples)

    /**
     * This metric calculates the number of long URIs.
     * It computes the ratio between the number of all long URIs
     * and the total number of URIs on the dataset.
     */
    def assessShortURIs(): Double =
      ShortURIs.assessShortURIs(triples)

    /**
     * Check if the incorrect numeric range for the given predicate and given class of subjects.
     * A user should specify the RDF class, the RDF property for which he would like to verify
     * if the values are in the specified range determined by the user.
     * The range is specified by the user by indicating the lower and the upper bound of the value.
     */
    def assessLiteralNumericRangeChecker(): Long =
      LiteralNumericRangeChecker.assessLiteralNumericRangeChecker(triples)

    /**
     * Check if the value of a typed literal is valid with regards to
     * the given xsd datatype.
     */
    def assessXSDDatatypeCompatibleLiterals(): Long =
      XSDDatatypeCompatibleLiterals.assessXSDDatatypeCompatibleLiterals(triples)

    /**
     * This metric assess the labeled resources.
     */
    def assessLabeledResources(): Double =
      LabeledResources.assessLabeledResources(triples)

  }
}
