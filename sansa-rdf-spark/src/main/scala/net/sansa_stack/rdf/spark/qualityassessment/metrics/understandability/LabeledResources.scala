package net.sansa_stack.rdf.spark.qualityassessment.metrics.understandability

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._

/**
 * @author Gezim Sejdiu
 */
object LabeledResources {
  implicit class LabeledResourcesFunctions(dataset: RDD[Triple]) extends Serializable {
    def assessLabeledResources() = {

      val triples = dataset.count().toDouble

      val subjects = dataset.filter(f => f.getSubject.isURI() && isInternal(f.getSubject) && isLabeled(f.getPredicate)).count().toDouble
      val predicates = dataset.filter(f => isInternal(f.getPredicate) && isLabeled(f.getPredicate)).count().toDouble
      val objects = dataset.filter(f => f.getObject.isURI() && isInternal(f.getObject) && isLabeled(f.getPredicate)).count.toDouble

      val labeledResources = subjects + predicates + objects

      val value = if (triples > 0.0)
        labeledResources / triples;
      else 0

      value
    }
  }
}
