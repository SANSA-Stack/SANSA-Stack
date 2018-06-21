package net.sansa_stack.rdf.spark.io

import net.sansa_stack.rdf.spark.model._
import org.apache.jena.graph.Node
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * Created by nilesh
 * Build an RDD of (s,p,o) coordinates, with integer IDs starting from 0 for representing URIs
 * Needed for training ML models on knowledge graphs
 * TODO docs
 */
class RDFSparseTensorReader(spark: SparkSession, path: String) {

  private val triplesWithURIs = {
    val graph = spark.rdf(Lang.NTRIPLES)(path)
    graph.filter { triple =>
      triple.getSubject.isURI() && triple.getPredicate.isURI() && triple.getObject.isURI()
    }
  }

  val relationIDs = triplesWithURIs.getPredicates().zipWithUniqueId()

  val entityIDs = (triplesWithURIs.getSubjects
    ++ triplesWithURIs.getObjects)
    .distinct
    .zipWithUniqueId()

  def getNumEntities: Long = entityIDs.count()

  def getNumRelations: Long = relationIDs.count()

  def getMappedTriples(): RDD[(Long, Long, Long)] = {
    val joinedBySubject = entityIDs.join(triplesWithURIs.map { triple =>
      (triple.getSubject(), (triple.getPredicate, triple.getObject))
    })

    val subjectMapped: RDD[(Long, Node, Node)] = joinedBySubject.map {
      case (_, _@ (subjectID: Long, _@ (predicate: Node, obj: Node))) =>
        (subjectID, predicate, obj)
    }

    val joinedByObject = entityIDs.join(subjectMapped.map {
      case (s, p, o) =>
        (o, (s, p))
    })

    val subjectObjectMapped = joinedByObject.map {
      case (_, _@ (objectID: Long, _@ (subjectID: Long, predicate: Node))) =>
        (subjectID, predicate, objectID)
    }

    val joinedByPredicate = relationIDs.join(subjectObjectMapped.map {
      case (s, p, o) =>
        (p, (s, o))
    })

    val allMapped = joinedByPredicate.map {
      case (_: Node, _@ (predicateID: Long, _@ (subjectID: Long, objectID: Long))) =>
        (subjectID, predicateID, objectID)
    }

    allMapped
  }
}
