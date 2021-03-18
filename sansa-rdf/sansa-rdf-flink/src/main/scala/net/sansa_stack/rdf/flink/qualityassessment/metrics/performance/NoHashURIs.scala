package net.sansa_stack.rdf.flink.qualityassessment.metrics.performance

import net.sansa_stack.rdf.common.qualityassessment.utils.NodeUtils._
import org.apache.flink.api.scala.DataSet
import org.apache.jena.graph.Triple

/**
 *
 * @author Gezim Sejdiu
 */
object NoHashURIs {

  def assessNoHashUris(dataset: DataSet[Triple]): Double = {

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
