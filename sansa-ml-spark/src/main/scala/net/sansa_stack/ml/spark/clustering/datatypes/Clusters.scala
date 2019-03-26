package net.sansa_stack.ml.spark.clustering.datatypes

/**
 * @param numOfClusters number of clusters
 * @param clusterSizes size of each cluster
 * @param clusters a list of cluster
 */
case class Clusters(numOfClusters: Int, clusterSizes: Array[Int], clusters: Array[Cluster])

