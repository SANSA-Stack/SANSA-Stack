package net.sansa_stack.ml.spark.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{PowerIterationClusteringModel,PowerIterationClustering}

class RDFGraphClustering(val data: RDD[String],
                         private val k: Int,
                         private val maxIterations: Int) extends Serializable {

  def clusterRdd(): RDD[(Long, Long, Double)] = {
    val splitRdd = data.map(line => line.split("\t"))
    val yourRdd = splitRdd.map(line => {
      val node1 = line(0).toLong
      val node2 = line(1).toLong
      val similarity = line(2).toDouble
      (node1, node2, similarity)
    })
    yourRdd
  }

  def run(): PowerIterationClusteringModel = {
    val model = new PowerIterationClustering()
      .setK(k)
      .setMaxIterations(maxIterations)
      .run(clusterRdd)
    model
  }
}

object RDFGraphClustering {
  def apply(data: RDD[String], k: Int, maxIterations: Int) = new RDFGraphClustering(data, k, maxIterations)
}