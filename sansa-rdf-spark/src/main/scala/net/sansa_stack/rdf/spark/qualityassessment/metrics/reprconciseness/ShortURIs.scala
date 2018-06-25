package net.sansa_stack.rdf.spark.qualityassessment.metrics.reprconciseness

import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.rdd.RDD

/**
 * @author Gezim Sejdiu
 */
object ShortURIs {

  /**
   * This metric calculates the number of long URIs.
   * It computes the ratio between the number of all long URIs
   * and the total number of URIs on the dataset.
   */
  def assessShortURIs(dataset: RDD[Triple]): Double = {

    val entities = dataset.filter(_.getSubject.isURI()).map(_.getSubject)
      .union(dataset.filter(_.getPredicate.isURI()).map(_.getPredicate))
      .union(dataset.filter(_.getObject.isURI()).map(_.getObject))
      .distinct()

    val allEntities = entities.count
    val shorestURIs = entities.filter(resourceTooLong(_)).count().toDouble

    if (allEntities >= 0.0) shorestURIs / allEntities else 0
  }
}
