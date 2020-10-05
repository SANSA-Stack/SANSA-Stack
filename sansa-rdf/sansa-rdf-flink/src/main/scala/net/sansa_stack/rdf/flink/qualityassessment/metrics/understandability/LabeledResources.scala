package net.sansa_stack.rdf.flink.qualityassessment.metrics.understandability

import net.sansa_stack.rdf.common.qualityassessment.utils.NodeUtils._
import org.apache.flink.api.scala._
import org.apache.jena.graph.{ Node, Triple }

/**
 * @author Gezim Sejdiu
 */
object LabeledResources {

  def assessLabeledResources(dataset: DataSet[Triple]): Double = {

    val triples = dataset.count().toDouble

    val subjects = dataset.filter(f =>
      f.getSubject.isURI() && isInternal(f.getSubject) &&
        isLabeled(f.getPredicate)).count().toDouble

    val predicates = dataset.filter(f =>
      isInternal(f.getPredicate) && isLabeled(f.getPredicate)).count().toDouble

    val objects = dataset.filter(f =>
      f.getObject.isURI() && isInternal(f.getObject) &&
        isLabeled(f.getPredicate)).count.toDouble

    val labeledResources = subjects + predicates + objects

    val value = if (triples > 0.0) {
      labeledResources / triples
    } else 0

    value
  }
}
