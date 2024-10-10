package net.sansa_stack.rdf.spark.model.tensor

import net.sansa_stack.rdf.spark.model._
import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.rdd.RDD

/**
 * Created by nilesh
 * Build an RDD of (s,p,o) coordinates, with integer IDs starting from 0 for representing URIs
 * Needed for training ML models on knowledge graphs
 * TODO docs
 */

object TripleOps {

  /**
   * Get all triples which contains URIs
   * @param triples  RDD of triples
   * @return RDD of triples with URIs only
   */
  def triplesWithURIs(triples: RDD[Triple]): RDD[Triple] = {
    triples.filter { triple =>
      triple.getSubject.isURI() && triple.getPredicate.isURI() && triple.getObject.isURI()
    }
  }

  /**
   * Return unique ID of the relations (predicates)
   * @param triplesWithURIs RDD of triples with URIs
   * @retun predicates and their unique IDs
   */
  def relationIDs(triplesWithURIs: RDD[Triple]): RDD[(Node, Long)] =
    triplesWithURIs.getPredicates().zipWithUniqueId()

  /**
   * Return unique IDs of all entities (subject, objects) in the graph
   * @param triplesWithURIs RDD of triples with URIs
   * @retun unique IDs of all entities (subject, objects) in the graph
   */
  def entityIDs(triplesWithURIs: RDD[Triple]): RDD[(Node, Long)] = {
    (triplesWithURIs.getSubjects() ++ triplesWithURIs.getObjects())
      .distinct()
      .zipWithUniqueId()
  }

  /**
   * Return size of the entities in the graph
   * @param triples RDD of triples
   * @return size of the entities in the graph
   */
  def getNumEntities(triples: RDD[Triple]): Long =
    entityIDs(triplesWithURIs(triples)).count()

  /**
   * Return size of the relations in the graph
   * @param triples RDD of triples
   * @return size of the relations in the graph
   */
  def getNumRelations(triples: RDD[Triple]): Long =
    relationIDs(triplesWithURIs(triples)).count()

  /**
   * Return all the mapped triples (tensor) based on their relations
   * @param triples RDD of triples
   * @return all the mapped triples (tensor) based on their relations
   */
  def getMappedTriples(triples: RDD[Triple]): RDD[(Long, Long, Long)] = {

    val _triplesWithURIs = triplesWithURIs(triples)
    val _entityIDs = entityIDs(_triplesWithURIs)
    val _relationIDs = relationIDs(_triplesWithURIs)

    val joinedBySubject = _entityIDs.join(_triplesWithURIs.map { triple =>
      (triple.getSubject(), (triple.getPredicate, triple.getObject))
    })

    val subjectMapped: RDD[(Long, Node, Node)] = joinedBySubject.map {
      case (_, _@ (subjectID: Long, _@ (predicate: Node, obj: Node))) =>
        (subjectID, predicate, obj)
    }

    val joinedByObject = _entityIDs.join(subjectMapped.map {
      case (s, p, o) =>
        (o, (s, p))
    })

    val subjectObjectMapped = joinedByObject.map {
      case (_, _@ (objectID: Long, _@ (subjectID: Long, predicate: Node))) =>
        (subjectID, predicate, objectID)
    }

    val joinedByPredicate = _relationIDs.join(subjectObjectMapped.map {
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
