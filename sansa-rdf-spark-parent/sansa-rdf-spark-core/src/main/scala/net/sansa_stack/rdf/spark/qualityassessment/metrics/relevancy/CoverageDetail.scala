package net.sansa_stack.rdf.spark.qualityassessment.metrics.relevancy

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Triple, Node }

/**
 * R2
 * This metric measures the the coverage (i.e. number of entities described
 * in a dataset) and level of detail (i.e. number of properties) in a dataset
 * to ensure that the data retrieved is appropriate for the task at hand.
 */
object CoverageDetail {
  implicit class CoverageDetailFunctions(dataset: RDD[Triple]) extends Serializable {
    def assessCoverageDetail() = {

      val triples = dataset.count().toDouble

      val predicates = dataset.map(_.getPredicate).distinct().count().toDouble

      val value = if (triples > 0.0)
        predicates / triples;
      else 0

      value

    }
  }
}
