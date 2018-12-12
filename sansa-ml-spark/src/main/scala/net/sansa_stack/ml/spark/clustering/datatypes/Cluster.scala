package net.sansa_stack.ml.spark.clustering.datatypes

/**
 * a cluster
 *
 * @param cluster_id id of cluster
 * @param poi_in_cluster an array of pois in cluster
 */
case class Cluster(cluster_id: Int, poi_in_cluster: Array[Poi])

