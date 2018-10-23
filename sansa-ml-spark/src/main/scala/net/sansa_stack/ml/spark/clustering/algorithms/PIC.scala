package net.sansa_stack.ml.spark.clustering.algorithms

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.rdd._
import org.apache.spark.sql._


class PIC {

  /*
   * Power Iteration clustering algorithm from Spark standard library
   * */
  def picSparkML(pairwisePOISimilarity: RDD[(Long, Long, Double)], numCentroids: Int, numIterations: Int, sparkSession: SparkSession): Map[Int, Array[Long]] = {
    val model = new PowerIterationClustering().setK(numCentroids).setMaxIterations(numIterations).setInitializationMode("degree").run(pairwisePOISimilarity)
    val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
    clusters
  }
/*
   * Power Iteration using implementation from SANSA
   * */
    def picSANSA(pairwisePOISimilarity: RDD[(Long, Long, Double)], numCentroids: Int, numIterations: Int, sparkSession: SparkSession) {
    val verticeS = pairwisePOISimilarity.map(f => f._1)
    val verticeD = pairwisePOISimilarity.map(f => f._2)
    val indexedMap = verticeS.union(verticeD).distinct().zipWithIndex()
    val vertices = indexedMap.map(f => (f._2, f._1))
    val edges = pairwisePOISimilarity.map(f => Edge(f._1, f._2, f._3)) // from similarity to int
    val similarityGraph = Graph(vertices, edges)
    // val model = new RDFGraphPICClustering(sparkSession, similarityGraph, numCentroids, numIterations)
  }
}


