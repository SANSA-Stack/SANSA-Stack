package net.sansa_stack.ml.spark.clustering.algorithms

import com.typesafe.config.ConfigFactory
import org.apache.jena.graph.Triple
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import net.sansa_stack.ml.spark.clustering.utils._


class Kmeans(input: RDD[Triple]) extends ClusterAlgo {
  var noofcluster = 0
  var noOfIter = 0
  var oneHotClusters = Map[Int, Array[Long]]()
  val spark = SparkSession.builder.getOrCreate()
  import spark.implicits._
  val conf = ConfigFactory.load()

  val dataProcessing = new DataProcessing(spark, conf, input)
  val pois = dataProcessing.pois
  def setK(K: Int): this.type = {
    noofcluster = K
    this
  }
  /**
   * set maximum iterations for Kmeans,PIC etc
   * @param iter
   */
  def setMaxIterations(iter: Int): this.type = {
    noOfIter = iter
    this
  }

  def run(): RDD[(Int, List[Triple])] = {
    val poiCategorySetVienna = pois.map(poi => (poi.poi_id, poi.categories.categories.toSet)).persist()
    val (oneHotDF, oneHotMatrix) = new Encoder().oneHotEncoding(poiCategorySetVienna, spark)
    oneHotClusters = kmClustering(
      numClusters = noofcluster,
      maxIter = noOfIter,
      df = oneHotDF,
      spark = spark)
    val temp = Common.seralizeToNT(spark.sparkContext, oneHotClusters, pois)
    temp
  }

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

object Kmeans {
  def apply(input: RDD[Triple]): Kmeans = new Kmeans(input)
 }


