package net.sansa_stack.rdf.spark.qualityassessment.metrics.completeness

import net.sansa_stack.rdf.spark.qualityassessment.utils.DatasetUtils._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.rdd.RDD

/**
 * @author Gezim Sejdiu
 */
object PropertyCompleteness {

  /**
   * This metric measures the property completeness by checking
   * the missing object values for the given predicate and given class of subjects.
   * A user specifies the RDF class and the RDF predicate, then it checks for each pair
   * whether instances of the given RDF class contain the specified RDF predicate.
   */
  def assessPropertyCompleteness(dataset: RDD[Triple]): Long = {

    /**
     * -->Rule->Filter-->
     * "select (?s) where ?s p=rdf:type o=Type; p2=Property ?o2
     * select (?s2) where ?s2 p=rdf:type o=Type"
     * -->Action-->
     * S+=?s && S2+=?s2
     * -->Post-processing-->
     * |S| / |S2|
     */
    val s2 = dataset.filter(f =>
      f.getPredicate.getLocalName.contains("type")
        && f.getObject.getLiteralLexicalForm.contains(subject)).cache()
    val s = s2.filter(_.getPredicate.getLiteralLexicalForm.contains(property))

    val S = s.map(_.getSubject).distinct().count()
    val S2 = s2.map(_.getSubject).distinct().count()

    if (S2 > 0) S / S2
    else 0
  }

}
