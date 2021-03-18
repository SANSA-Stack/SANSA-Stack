package net.sansa_stack.rdf.flink.qualityassessment.metrics.reprconciseness

import net.sansa_stack.rdf.common.qualityassessment.utils.NodeUtils._
import org.apache.flink.api.scala._
import org.apache.jena.graph.Triple

/**
 * @author Gezim Sejdiu
 */
object ShortURIs {

  /**
   * This metric calculates the number of long URIs.
   * It computes the ratio between the number of all long URIs
   * and the total number of URIs on the dataset.
   */
  def assessShortURIs(dataset: DataSet[Triple]): Double = {

    val entities = dataset.filter(_.getSubject.isURI()).map(_.getSubject)
      .union(dataset.filter(_.getPredicate.isURI()).map(_.getPredicate))
      .union(dataset.filter(_.getObject.isURI()).map(_.getObject))
      .distinct(_.hashCode())

    val allEntities = entities.count
    val shorestURIs = entities.filter(resourceTooLong(_)).count().toDouble

    if (allEntities >= 0.0) shorestURIs / allEntities else 0
  }
}

