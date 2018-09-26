package net.sansa_stack.ml.spark.clustering.poi

import java.io.PrintWriter

import org.apache.spark.sql._
import net.sansa_stack.ml.spark.clustering.algorithms.{ Distances, Encoder, Kmeans, PIC }
import net.sansa_stack.ml.spark.clustering.datatypes._
import net.sansa_stack.ml.spark.clustering.utils.dataProcessing
import net.sansa_stack.ml.spark.clustering.utils.Common
import org.json4s._
import com.typesafe.config.ConfigFactory
import org.json4s.jackson.Serialization

object poiClustering {

  /**
   * main function
   */
  def main(args: Array[String]) {
    implicit val formats = DefaultFormats
    val conf = ConfigFactory.load()
    val profileWriter = new PrintWriter(conf.getString("slipo.clustering.profile"))
    val picFileWriter = new PrintWriter(conf.getString("slipo.clustering.pic.result"))
    val oneHotKMFileWriter = new PrintWriter(conf.getString("slipo.clustering.km.onehot.result"))
    val mdsKMFileWriter = new PrintWriter(conf.getString("slipo.clustering.km.mds.result"))
    val word2VecKMFileWriter = new PrintWriter(conf.getString("slipo.clustering.km.word2vec.result"))
    val picDistanceMatrixWriter = new PrintWriter(conf.getString("slipo.clustering.pic.matrix"))
    val mdsCoordinatesWriter = new PrintWriter(conf.getString("slipo.clustering.km.mds.matrix"))
    val oneHotMatrixWriter = new PrintWriter(conf.getString("slipo.clustering.km.onehot.matrix"))
    val word2VecWriter = new PrintWriter(conf.getString("slipo.clustering.km.word2vec.matrix"))

    // System.setProperty("hadoop.home.dir", "C:\\Hadoop") // for Windows system
    val spark = SparkSession.builder
      .master(conf.getString("slipo.spark.master"))
      .config("spark.serializer", conf.getString("slipo.spark.serializer"))
      .config("spark.executor.memory", conf.getString("slipo.spark.executor.memory"))
      .config("spark.driver.memory", conf.getString("slipo.spark.driver.memory"))
      .config("spark.driver.maxResultSize", conf.getString("slipo.spark.driver.maxResultSize"))
      // .config("spark.memory.fraction", conf.getString("spark.memory.fraction"))
      .appName(conf.getString("slipo.spark.app.name"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    println(spark.conf.getAll.mkString("\n"))
    val t0 = System.nanoTime()
    val tomTomData = new dataProcessing(spark = spark, conf = conf)

    val pois = tomTomData.pois
    println(pois.count())
    // val poiCategorySetVienna = tomTomData.poiCategoryId
    val poiCategorySetVienna = pois.map(poi => (poi.poi_id, poi.categories.categories.toSet))
    println("poiCategorySetVienna starts")
    poiCategorySetVienna.foreach(println)
    println("poiCategorySetVienna starts end")
    profileWriter.println(pois.count())
    profileWriter.println(poiCategorySetVienna.count())
    val t1 = System.nanoTime()
    profileWriter.println("Elapsed time preparing data: " + (t1 - t0) / 1000000000 + "s")

    // one hot encoding
    println("Start one hot encoding km")
    val (oneHotDF, oneHotMatrix) = new Encoder().oneHotEncoding(poiCategorySetVienna, spark)
    Serialization.writePretty(oneHotMatrix, oneHotMatrixWriter)
    val oneHotClusters = new Kmeans().kmClustering(
      numClusters = conf.getInt("slipo.clustering.km.onehot.number_clusters"),
      maxIter = conf.getInt("slipo.clustering.km.onehot.iterations"),
      df = oneHotDF,
      spark = spark)
    Common.writeClusteringResult(spark.sparkContext, oneHotClusters, pois, oneHotKMFileWriter)
    val t2 = System.nanoTime()
    profileWriter.println("Elapsed time one hot: " + (t2 - t0) / 1000000000 + "s")
    println("End one hot encoding km")

    // word2Vec encoding
    println("Start word2vec encoding km")
    val (avgVectorDF, word2Vec) = new Encoder().wordVectorEncoder(poiCategorySetVienna, spark)
    Serialization.writePretty(word2Vec.collect(), word2VecWriter)
    val avgVectorClusters = new Kmeans().kmClustering(
      numClusters = conf.getInt("slipo.clustering.km.word2vec.number_clusters"),
      maxIter = conf.getInt("slipo.clustering.km.word2vec.iterations"),
      df = avgVectorDF,
      spark = spark)
    Common.writeClusteringResult(spark.sparkContext, avgVectorClusters, pois, word2VecKMFileWriter)
    val t3 = System.nanoTime()
    profileWriter.println("Elapsed time word2Vec: " + (t3 - t0) / 1000000000 + "s")
    println("End one hot encoding km")

    println("Start PIC")
    // pic clustering, build ((sid, ()), (did, ())) RDD
    val pairwisePOICategorySet = poiCategorySetVienna.cartesian(poiCategorySetVienna).filter { case (a, b) => a._1 < b._1 }
    // from ((sid, ()), (did, ())) to (sid, did, similarity)
    val pairwisePOISimilarity = pairwisePOICategorySet.map(x => (x._1._1.toLong, x._2._1.toLong,
      new Distances().jaccardSimilarity(x._1._2, x._2._2))).persist()

    val picDistanceMatrix = DistanceMatrix(pairwisePOISimilarity.map(x => Distance(x._1, x._2, 1 - x._3)).collect().toList)
    Serialization.writePretty(picDistanceMatrix, picDistanceMatrixWriter)
    val clustersPIC = new PIC().picSparkML(
      pairwisePOISimilarity,
      conf.getInt("slipo.clustering.pic.number_clusters"),
      conf.getInt("slipo.clustering.pic.iterations"),
      spark)
    Common.writeClusteringResult(spark.sparkContext, clustersPIC, pois, picFileWriter)
    val t4 = System.nanoTime()
    profileWriter.println("Elapsed time cartesian: " + (t4 - t0) / 1000000000 + "s")

    println("End PIC")

    println("Start MDS")
    // distance RDD, from (sid, did, similarity) to (sid, did, distance)
    val distancePairs = pairwisePOISimilarity.map(x => (x._1, x._2, 1.0 - x._3)).persist()
    val (mdsDF, coordinates) = new Encoder().mdsEncoding(
      distancePairs = distancePairs,
      poiCategorySetVienna.count().toInt,
      dimension = conf.getInt("slipo.clustering.km.mds.dimension"),
      spark = spark)
    val mdsCoordinates = MdsCoordinates(coordinates.map(f => MdsCoordinate(f._1, f._2)))
    Serialization.writePretty(mdsCoordinates, mdsCoordinatesWriter)
    val mdsClusters = new Kmeans().kmClustering(
      numClusters = conf.getInt("slipo.clustering.km.mds.number_clusters"),
      maxIter = conf.getInt("slipo.clustering.km.mds.iterations"),
      df = mdsDF,
      spark = spark)
    Common.writeClusteringResult(spark.sparkContext, mdsClusters, pois, mdsKMFileWriter)
    val t5 = System.nanoTime()
    profileWriter.println("Elapsed time mds: " + (t5 - t0) / 1000000000 + "s")
    println("End MDS")
    // dbscan clustering, TODO solve scala version flicts with SANSA
    // dbscanClustering(coordinates, spark)
    picFileWriter.close()
    oneHotKMFileWriter.close()
    mdsKMFileWriter.close()
    word2VecKMFileWriter.close()
    profileWriter.close()
    picDistanceMatrixWriter.close()
    mdsCoordinatesWriter.close()
    oneHotMatrixWriter.close()
    word2VecWriter.close()
    spark.stop()
  }
}
