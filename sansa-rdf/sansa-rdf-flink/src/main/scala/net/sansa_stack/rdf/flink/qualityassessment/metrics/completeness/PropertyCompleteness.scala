package net.sansa_stack.rdf.flink.qualityassessment.metrics.completeness

import net.sansa_stack.rdf.common.qualityassessment.utils.DatasetUtils._
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.vocabulary.RDF

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
  def assessPropertyCompleteness(triples: DataSet[Triple]): Long = {

    /**
      * -->Rule->Filter-->
      * "select (?s) where ?s p=rdf:type o=Type; p2=Property ?o2
      * select (?s2) where ?s2 p=rdf:type o=Type"
      * -->Action-->
      * S+=?s && S2+=?s2
      * -->Post-processing-->
      * |S| / |S2|
      */
    val s2 = triples.filter(f =>
      f.getPredicate.matches(RDF.`type`.asNode())
        && f.getObject.matches(NodeFactory.createURI(subject)))

    val s = s2.filter(_.getPredicate.matches(NodeFactory.createURI(property)))

    val S = s.map(_.getSubject).distinct(_.hashCode()).count()
    val S2 = s2.map(_.getSubject).distinct(_.hashCode()).count()

    if (S2 > 0) S / S2
    else 0
  }

}
