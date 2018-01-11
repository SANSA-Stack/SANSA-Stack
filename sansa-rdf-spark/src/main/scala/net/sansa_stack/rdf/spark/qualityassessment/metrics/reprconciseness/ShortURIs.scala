package net.sansa_stack.rdf.spark.qualityassessment.metrics.reprconciseness

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.{ Triple, Node }
import net.sansa_stack.rdf.spark.qualityassessment.utils.NodeUtils._

object ShortURIs {
  implicit class ShortURIsFunctions(dataset: RDD[Triple]) extends Serializable {

    /**
     * This metric calculates the number of long URIs.
     * It computes the ratio between the number of all long URIs
     * and the total number of URIs on the dataset.
     */
    def assessShortURIs() = {

      val entities = dataset.filter(_.getSubject.isURI()).map(_.getSubject)
        .union(dataset.filter(_.getPredicate.isURI()).map(_.getPredicate))
        .union(dataset.filter(_.getObject.isURI()).map(_.getObject))
        .distinct()
      entities.cache()

      val allEntities = entities.count
      val shorestURIs = entities.filter(resourceTooLong(_)).count().toDouble

      if (allEntities >= 0.0) shorestURIs / allEntities else 0
    }

  }
}