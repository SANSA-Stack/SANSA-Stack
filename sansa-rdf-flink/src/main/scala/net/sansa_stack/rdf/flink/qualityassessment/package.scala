package net.sansa_stack.rdf.flink

import net.sansa_stack.rdf.flink.qualityassessment.metrics.availability._
import net.sansa_stack.rdf.flink.qualityassessment.metrics.completeness._
import net.sansa_stack.rdf.flink.qualityassessment.metrics.syntacticvalidity._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet

/**
 * @author Gezim Sejdiu
 */
package object qualityassessment {

  implicit class QualityAssessmentOperations(triples: DataSet[Triple]) {

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

  }

}