package net.sansa_stack.rdf.spark.qualityassessment.metrics.relevancy

import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.rdd.RDD

/**
 * @author Gezim Sejdiu
 */
object CoverageDetail {

  /**
   * R2
   * This metric measures the the coverage (i.e. number of entities described
   * in a dataset) and level of detail (i.e. number of properties) in a dataset
   * to ensure that the data retrieved is appropriate for the task at hand.
   */
  def assessCoverageDetail(dataset: RDD[Triple]): Double = {

    val triples = dataset.count().toDouble

    val predicates = dataset.map(_.getPredicate).distinct().count().toDouble

    val value = if (triples > 0.0) {
      predicates / triples
    } else 0

    value

  }
}
