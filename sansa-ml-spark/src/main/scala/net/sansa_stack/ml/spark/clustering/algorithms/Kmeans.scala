package net.sansa_stack.ml.spark.clustering.algorithms

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

class Kmeans {

  /**
   * K-means clustering based on given Dataframe
   *
   * @param numClusters
   * @param df
   * @param spark
   * @return cluster id and corresponding pois in cluster
   */
  def kmClustering(numClusters: Int, maxIter: Int, df: DataFrame, spark: SparkSession): Map[Int, Array[Long]] = {
    val km = new KMeans().setK(numClusters).setMaxIter(maxIter).setSeed(1L).setFeaturesCol("features").setPredictionCol("prediction")
    val model = km.fit(df)
    val transformedDataFrame = model.transform(df)
    import spark.implicits._
    // get (cluster_id, poi_id)
    val clusterIdPoi = transformedDataFrame.map(f => (f.getInt(f.size - 1), f.getInt(0).toLong)).rdd.groupByKey()
    val clustersMDSKM = clusterIdPoi.map(x => (x._1, x._2.toArray)).collectAsMap().toMap
    clustersMDSKM
  }
}

