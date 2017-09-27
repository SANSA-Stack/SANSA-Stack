package net.sansa_stack.ml.kge

import net.sansa_stack.rdf.spark.model.{JenaSparkRDD, JenaSparkRDDOps}
import net.sansa_stack.rdf.spark.model.TripleRDD._
import net.sansa_stack.rdf.spark.model.JenaSparkRDD
import org.apache.jena.graph.Node_URI
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import ml.dmlc.mxnet._

/**
 * Created by nilesh
 * Build an RDD of (s,p,o) coordinates, with integer IDs starting from 0 for representing URIs
 * Needed for training ML models on knowledge graphs
 * TODO docs
 */
class RDFSparseTensorReader(sc: SparkContext, path: String) {
 type Node = JenaSparkRDD#Node

 private val ops = JenaSparkRDDOps(sc)
 import ops._

 private val triplesWithURIs = {
   val graph = ops.loadGraphFromNTriples(path, "")
   graph.filter{
     case Triple(s, p, o) =>
       s.isURI && p.isURI && o.isURI
   }
 }

 val relationIDs = triplesWithURIs.getPredicates.zipWithUniqueId()

 val entityIDs = (triplesWithURIs.getSubjects
   ++ triplesWithURIs.getObjects)
   .distinct
   .zipWithUniqueId()

 def getNumEntities = entityIDs.count()

 def getNumRelations = relationIDs.count()

 def getMappedTriples(): Unit = {
   val joinedBySubject = entityIDs.join(triplesWithURIs.map{
     case Triple(s, p, o) =>
       (s, (p, o))
   })

   val subjectMapped: RDD[(Long, Node_URI, Node)] = joinedBySubject.map{
     case (_, _ @ (subjectID: Long, _ @ (predicate: Node_URI, obj: Node))) =>
       (subjectID, predicate, obj)
   }

   val joinedByObject = entityIDs.join(subjectMapped.map{
     case (s, p, o) =>
       (o, (s, p))
   })

   val subjectObjectMapped = joinedByObject.map{
     case (_, _ @ (objectID: Long, _ @ (subjectID: Long, predicate: Node_URI))) =>
       (subjectID, predicate, objectID)
   }

   val joinedByPredicate = relationIDs.join(subjectObjectMapped.map{
     case (s, p, o) =>
       (p, (s, o))
   })

   val allMapped = joinedByPredicate.map{
     case (_: Node, _ @ (predicateID: Long, _ @ (subjectID: Long, objectID: Long))) =>
       (subjectID, predicateID, objectID)
   }

   allMapped
 }
}
