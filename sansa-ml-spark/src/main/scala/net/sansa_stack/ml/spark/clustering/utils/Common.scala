package net.sansa_stack.ml.spark.clustering.utils

import java.io.PrintWriter

import org.apache.jena.graph.{ NodeFactory, Triple}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import net.sansa_stack.ml.spark.clustering.datatypes.{Cluster, Clusters, Poi}

object Common {
  val prefixID = "http://example.org/id/poi/"
  val prefixCategory = "http://example.org/hasCategory"
  val prefixCoordinate = "http://example.org/id/hasCoordinate/"


  /**
    * create a pair RDD and join with another pair RDD
    *
    * @param sparkContext
    * @param ids an array with poi id
    * @param pairs
    * @return an array of poi
    */
  def join(sparkContext: SparkContext, ids: Array[Long], pairs: RDD[(Long, Poi)]): Array[Poi] = {
    val idsPair = sparkContext.parallelize(ids).map(x => (x, x))
    idsPair.join(pairs).map(x => x._2._2).collect()
  }

  /**
    * serialize clustering results to file
    *
    * @param sparkContext
    * @param clusters clustering results
    * @param pois pois object
    * @return
    */
  def writeClusteringResult(sparkContext: SparkContext, clusters: Map[Int, Array[Long]], pois: RDD[Poi], fileWriter: PrintWriter): Unit = {
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val poisKeyPair = pois.keyBy(f => f.poi_id).persist()
    val clustersPois = Clusters(assignments.size, assignments.map(_._2.length).toArray, assignments.map(f => Cluster(f._1, join(sparkContext, f._2, poisKeyPair))))
    implicit val formats = DefaultFormats
    Serialization.writePretty(clustersPois, fileWriter)
  }
   /**
    * serialize clustering results to .nt file
    */
  def seralizeToNT(sparkContext: SparkContext, clusters: Map[Int, Array[Long]], pois: RDD[Poi]): Unit = {
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val poisKeyPair = pois.keyBy(f => f.poi_id).persist()
    val newAssignment = assignments.map(f => (f._1, sparkContext.parallelize(f._2).map(x => (x, x)).join(poisKeyPair).map(x => ( x._2._2.poi_id, x._2._2.categories, x._2._2.coordinate)).collect()))
    val newAssignmentRDD = sparkContext.parallelize(newAssignment)
    println(newAssignmentRDD.count())
    val newAssignmentRDDTriple = newAssignmentRDD.map(cluster => (cluster._1, cluster._2.flatMap(poi =>
                                          {List(new Triple(NodeFactory.createURI(prefixID + poi._1.toString),
                                                    NodeFactory.createURI(prefixCategory),
                                                    NodeFactory.createLiteral(poi._2.categories.mkString(","))),
                                                new Triple(NodeFactory.createURI(prefixID + poi._1.toString),
                                                  NodeFactory.createURI(prefixCoordinate),
                                                  NodeFactory.createLiteral((poi._3.latitude, poi._3.longitude).toString()))
                                          )}
                                            ).toList)
    )
    newAssignmentRDDTriple.saveAsTextFile("results/triples")
  }

}


