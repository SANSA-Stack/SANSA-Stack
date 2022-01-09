package net.sansa_stack.ml.spark.anomalydetection

import net.sansa_stack.ml.spark.anomalydetection.DistADLogger.LOG
import net.sansa_stack.ml.spark.utils.FeatureExtractorModel
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.jena.graph
import org.apache.spark.HashPartitioner
import org.apache.spark.ml.feature.{
  CountVectorizer,
  CountVectorizerModel,
  MinHashLSH,
  MinHashLSHModel
}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * This is the modified version of CONOD which can work on any arbitrary RDF data.
  * The original CONOD was only working on DBpedia. By changing the similarity function now it is generic
  * @param spark the initiated Spark session
  * @param originalDataRDD the data
  * @param config the config object
  */
class CONOD(
    spark: SparkSession,
    originalDataRDD: RDD[graph.Triple],
    config: DistADConfig
) extends Serializable {

  /**
    * The main function
    * @return the dataframe containing all the anomalies
    */
  def run(): DataFrame = {
    val now = System.currentTimeMillis()

    var clusterOfSubject: RDD[(Set[(String, String, Double)])] = null
    val getObjectLiteral = DistADUtil.getOnlyLiteralObjects(originalDataRDD)
    if (config.verbose) {
      LOG.info("List of all triple which their objects are literals")
      getObjectLiteral.take(10) foreach LOG.info
    }
    val removedLangString = getObjectLiteral.filter(
      f => DistADUtil.searchEdge(f.getObject.toString(), DistADUtil.objList)
    )
    if (config.verbose) {
      LOG.info("List of all triple which their objects are numerical literals")
      removedLangString.take(10) foreach LOG.info
    }

    val triplesWithNumericLiteral =
      DistADUtil.triplesWithNumericLit(removedLangString)
    val mapSubWithTriples =
      propClustering(
        triplesWithNumericLiteral
      )
    if (config.verbose) {
      LOG.info("Group all the numeric features")
      mapSubWithTriples.take(10) foreach LOG.info
    }

    clusterOfSubject = jSimilarity(
      triplesWithNumericLiteral,
      mapSubWithTriples
    )

    if (config.verbose) {
      LOG.info("Clusters")
      clusterOfSubject.take(10) foreach LOG.info
    }

    val setData: RDD[Set[(String, String, Double)]] =
      clusterOfSubject.repartition(1000).persist(StorageLevel.MEMORY_AND_DISK)
    val setDataStore: RDD[Seq[(String, String, Double)]] =
      setData.map(f => f.toSeq)

    val setDataSize = setDataStore.filter(f => f.size > config.anomalyListSize)
    val test = setDataSize.map(f => iqr(f, config.anomalyListSize))
    val testfilter = test.filter(f => f.size > 0)
    val testfilterDistinct: RDD[(String, String, Double)] =
      testfilter.flatMap(f => f)

    import spark.sqlContext.implicits._

    val finalResult = testfilterDistinct
      .map({
        case (val1: String, val2: String, val3: Double) =>
          (val1, val2, val3.toString)
      })
      .toDF("s", "p", "o")

    LOG.info("Operation took: " + (System.currentTimeMillis() - now))
    finalResult

  }

  /** Anomaly Detection method based on Interquartile Range
    * @param data
    *   a given data
    * @param anomalyListLimit
    *   the min value list size for considering a list for anomaly detection
    *   process
    * @return
    *   list of datapoints which are anomalies
    */
  def iqr(
      data: Seq[(String, String, Double)],
      anomalyListLimit: Int
  ): Seq[(String, String, Double)] = {

    val listofData =
      data.map(b => (b._3.toString().replace("\"", "")).toDouble).toArray
    val c = listofData.sorted
    val arrMean = new DescriptiveStatistics()
    genericArrayOps(c).foreach(v => arrMean.addValue(v))
    val Q1 = arrMean.getPercentile(25)
    val Q3 = arrMean.getPercentile(75)
    val IQR = Q3 - Q1
    val lowerRange = Q1 - 1.5 * IQR
    val upperRange = Q3 + 1.5 * IQR
    val yse = c.filter(p => (p < lowerRange || p > upperRange))
    val xde =
      data.filter(
        f => DistADUtil.search(f._3.toString().replace("\"", "").toDouble, yse)
      )
    xde
  }

  /** Map the RDD[Triple] to local names of subject and predicate
    * @param a
    *   a given RDD[Triple]
    * @return
    *   RDD[(String, String)]
    */
//  def propWithSubject(a: RDD[graph.Triple]): RDD[(String, String)] =
//    a.map(
//      f =>
//        (
//          DistADUtil.getLocalName(f.getSubject),
//          DistADUtil.getLocalName(f.getPredicate)
//        )
//    )

  /** @param triplesWithNumericLiteral
    * @return
    */
  def propClustering(
      triplesWithNumericLiteral: RDD[graph.Triple]
  ): RDD[(String, mutable.Set[(String, String, Double)])] = {

    val subMap = triplesWithNumericLiteral.map(
      f =>
        (
          f.getSubject.toString,
          (
            f.getSubject.toString,
            f.getPredicate.toString,
            DistADUtil.getNumber(f.getObject.toString())
          )
        )
    )
    val initialSet = mutable.Set.empty[(String, String, Double)]
    val addToSet =
      (s: mutable.Set[(String, String, Double)], v: (String, String, Double)) =>
        s += v
    val mergePartitionSets = (
        p1: mutable.Set[(String, String, Double)],
        p2: mutable.Set[(String, String, Double)]
    ) => p1 ++= p2
    val uniqueByKey =
      subMap.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    uniqueByKey
  }

  /** @param TriplesWithNumericLiteral
    * @param mapSubWithTriples
    * @return
    */
  def jSimilarity(
      TriplesWithNumericLiteral: RDD[graph.Triple],
      mapSubWithTriples: RDD[(String, mutable.Set[(String, String, Double)])]
  ): RDD[(Set[(String, String, Double)])] = {

    val validEntitySet =
      TriplesWithNumericLiteral.map(f => f.getSubject).cache().collect().toSet

    val nTriplesRDDWithValidEntities =
      originalDataRDD.filter(f => validEntitySet.contains(f.getSubject))

    val pairwiseSim = calculateSimilarity(
      nTriplesRDDWithValidEntities
    )

    val opiu: DataFrame = pairwiseSim
      .filter(col("datasetA.uri").isNotNull)
      .filter(col("datasetB.uri").isNotNull)
      .filter((col("datasetA.uri") =!= col("datasetB.uri")))
      .select(
        col("datasetA.uri").alias("id1"),
        col("datasetB.uri").alias("id2")
      )

    val x1 = opiu.persist(StorageLevel.MEMORY_AND_DISK)

    val x1Map = x1.rdd.map(row => {
      val id = row.getString(0)
      val value = row.getString(1)
      (id, value)
    })

    val initialSet3 = mutable.Set.empty[String]
    val addToSet3 = (s: mutable.Set[String], v: String) => s += v
    val mergePartitionSets3 =
      (p1: mutable.Set[String], p2: mutable.Set[String]) => p1 ++= p2
    val uniqueByKey3 =
      x1Map.aggregateByKey(initialSet3)(addToSet3, mergePartitionSets3)

    val k = uniqueByKey3.map(f => ((f._1, (f._2 += (f._1)).toSet)))

    val partitioner = new HashPartitioner(500)
    val mapSubWithTriplesPart = mapSubWithTriples
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val ys = k.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)
    val joinSimSubTriples2 = ys.join(mapSubWithTriplesPart)

    val clusterOfSubjects = joinSimSubTriples2.map({
      case (s, (iter, iter1)) =>
        ((iter).toSet, iter1)
    })

    val initialSet =
      mutable.HashSet.empty[mutable.Set[(String, String, Double)]]
    val addToSet = (
        s: mutable.HashSet[mutable.Set[(String, String, Double)]],
        v: mutable.Set[(String, String, Double)]
    ) => s += v
    val mergePartitionSets = (
        p1: mutable.HashSet[mutable.Set[(String, String, Double)]],
        p2: mutable.HashSet[mutable.Set[(String, String, Double)]]
    ) => p1 ++= p2
    val uniqueByKey =
      clusterOfSubjects.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    val propCluster = uniqueByKey.map({
      case (a, (iter)) =>
        (iter.flatMap(f => f.map(f => f._2)).toSet, ((iter)))
    })

    val propDistinct = propCluster.flatMap {
      case (a, ((iter))) =>
        a.map(f => (f, ((iter.flatMap(f => f).toSet))))
    }

    val clusterOfProp = propDistinct.map({
      case (a, (iter1)) => (iter1.filter(f => f._2.equals(a)))
    })

    mapSubWithTriplesPart.unpersist()
    ys.unpersist()

    clusterOfProp
  }

  /** @param triplesRDD
    * @return
    */
  private def calculateSimilarity(triplesRDD: RDD[graph.Triple]) = {
    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("or")
    val extractedFeaturesDataFrame: DataFrame = featureExtractorModel
      .transform(triplesRDD)

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .fit(extractedFeaturesDataFrame)
    val tmpCvDf: DataFrame = cvModel.transform(extractedFeaturesDataFrame)

    val isNoneZeroVector =
      udf({ (v: Vector) =>
        v.numNonzeros > 0
      })

    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf
      .filter(isNoneZeroVector(col("vectorizedFeatures")))
      .select("uri", "vectorizedFeatures")

    val countVectorizedFeaturesDataFramePartitioned =
      countVectorizedFeaturesDataFrame.cache()

    countVectorizedFeaturesDataFramePartitioned.collect()

    val minHashModel: MinHashLSHModel = new MinHashLSH()
      .setNumHashTables(3)
      .setInputCol("vectorizedFeatures")
      .setOutputCol("hashValues")
      .fit(countVectorizedFeaturesDataFramePartitioned)

    val pairwiseSim = minHashModel
      .approxSimilarityJoin(
        countVectorizedFeaturesDataFramePartitioned,
        countVectorizedFeaturesDataFramePartitioned,
        config.pairWiseDistanceThreshold,
        "distance"
      )

    pairwiseSim
  }
}
