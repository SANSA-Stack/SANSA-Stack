package net.sansa_stack.ml.spark.clustering.utils

import java.io.PrintWriter

import net.sansa_stack.ml.spark.clustering.datatypes.{Cluster, Clusters, Poi}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

object Common {

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

}
