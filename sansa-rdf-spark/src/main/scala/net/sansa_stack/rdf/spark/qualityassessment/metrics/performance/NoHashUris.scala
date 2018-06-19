package net.sansa_stack.rdf.spark.qualityassessment.metrics.performance

import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.rdd.RDD

/**
 * @author Gezim Sejdiu
 */
object NoHashURIs {

  def assessNoHashUris(dataset: RDD[Triple]): Double = {

    val triples = dataset.count().toDouble

    val subjects = dataset.filter(f => f.getSubject.isURI() && isHashUri(f.getSubject)).count().toDouble
    val predicate = dataset.filter(f => isHashUri(f.getPredicate)).count().toDouble
    val objects = dataset.filter(f => f.getObject.isURI() && isHashUri(f.getObject)).count().toDouble

    val NoHashURIs = subjects + predicate + objects

    val value = if (triples > 0.0) {
      NoHashURIs / triples
    } else 0

    value
  }
}
