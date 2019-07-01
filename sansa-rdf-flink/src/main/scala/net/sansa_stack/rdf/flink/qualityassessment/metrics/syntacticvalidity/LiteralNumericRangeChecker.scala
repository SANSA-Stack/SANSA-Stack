package net.sansa_stack.rdf.flink.qualityassessment.metrics.syntacticvalidity

import net.sansa_stack.rdf.common.qualityassessment.utils.DatasetUtils._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.jena.graph.{ Node, Triple }

/**
 * @author Gezim Sejdiu
 */
object LiteralNumericRangeChecker {

  /**
   * Check if the incorrect numeric range for the given predicate and given class of subjects.
   * A user should specify the RDF class, the RDF property for which he would like to verify
   * if the values are in the specified range determined by the user.
   * The range is specified by the user by indicating the lower and the upper bound of the value.
   */
  def assessLiteralNumericRangeChecker(triples: DataSet[Triple]): Long = {

    /**
     * -->Rule->Filter-->
     * "select COUNT(?s) where ?s rdf:type o=Type .
     * select COUNT(?s2) where ?s2 p=rdf:type o=Type ?s <"+ property +"> ?o . FILTER (?o > "+ lowerBound +" && ?o < "+ upperBound +") ."
     * -->Action-->
     * S+=?s && S2+=?s2
     * -->Post-processing-->
     * |S| / |S2|
     */

    val s2 = triples.filter(f =>
      f.getPredicate.getLiteralLexicalForm.contains("rdf:type")
        && f.getSubject.getLiteralLexicalForm.contains(subject))

    val s = s2.filter(f => f.getPredicate.getLiteralLexicalForm.contains(property)
      && (f.getObject.getLiteralValue.toString().toDouble > lowerBound && f.getObject.getLiteralValue.toString().toDouble < upperBound))

    val S = s.distinct().count()
    val S2 = s2.distinct().count()

    val accuracy = if (S2 > 0) S / S2 else 0

    accuracy
  }
}
