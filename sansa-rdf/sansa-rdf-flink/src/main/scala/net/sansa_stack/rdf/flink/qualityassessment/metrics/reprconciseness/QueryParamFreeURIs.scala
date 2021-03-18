package net.sansa_stack.rdf.flink.qualityassessment.metrics.reprconciseness

import net.sansa_stack.rdf.common.qualityassessment.utils.NodeUtils._
import org.apache.flink.api.scala._
import org.apache.jena.graph.Triple

/**
  * @author Gezim Sejdiu
  */
object QueryParamFreeURIs {

  /**
    * This metric calculates the number of non Queryable URIs.
    * It computes the ratio between the number of all non queryable URIs
    * and the total number of URIs on the dataset.
    */
  def assessQueryParamFreeURIs(dataset: DataSet[Triple]): Double = {

    val entities = dataset.filter(_.getSubject.isURI()).map(_.getSubject)
      .union(dataset.filter(_.getPredicate.isURI()).map(_.getPredicate))
      .union(dataset.filter(_.getObject.isURI()).map(_.getObject))
      .distinct(_.hashCode())

    val allEntities = entities.count
    val queryParamFreeURIs = entities.filter(hasQueryString(_)).count().toDouble

    if (allEntities >= 0.0) queryParamFreeURIs / allEntities else 0
  }
}
