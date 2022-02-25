package net.sansa_stack.ml.spark.anomalydetection

import com.linkedin.relevance.isolationforest.IsolationForest
import net.sansa_stack.ml.spark.anomalydetection.DistADLogger.LOG
import net.sansa_stack.ml.spark.featureExtraction.FeatureExtractingSparqlGenerator.createSparql
import net.sansa_stack.ml.spark.featureExtraction.SparqlFrame
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/** This class runs multi-feature anomaly detection based on IsolationForest
  * algorithm
  * @param spark
  *   the initiated Spark session
  * @param originalDataRDD
  *   RDD[Triple]
  * @param config
  *   the config file object
  */
class MultiFeatureAnomalyDetection(
    spark: SparkSession,
    originalDataRDD: RDD[graph.Triple],
    config: DistADConfig
) {

  /**
    * The main function
    * @return the dataframe containing all the anomalies
    */
  def run(): DataFrame = {

    val startTime = System.currentTimeMillis()

    val onlyLiteralDataRDD: RDD[graph.Triple] =
      DistADUtil.triplesWithNumericLitWithTypeIgnoreEndingWithID(
        originalDataRDD
      )
    if (config.verbose) {
      LOG.info("Original Data RDD Only with numeric Literals:")
      onlyLiteralDataRDD.take(10) foreach LOG.info
    }

    var onlyLiteralDataDataSet: Dataset[graph.Triple] = null
    if (config.featureExtractor.equals(config.LITERAL2FEATURE)) {
      implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node)])
      onlyLiteralDataDataSet = onlyLiteralDataRDD.toDS()
    }
    val onlyLiteralDataDataFrame =
      DistADUtil
        .createDFWithConversion(onlyLiteralDataRDD)
        .cache()
    if (config.verbose) {
      LOG.info("Original Data DataFrame Only with numeric Literals:")
      onlyLiteralDataDataFrame.show(false)
    }

    val onlyLiteralDataWithDoubleDataFramePivoted =
      config.featureExtractor match {
        case config.PIVOT =>
          var onlyLiteralDataWithDoubleDataFramePivoted =
            onlyLiteralDataDataFrame
              .groupBy("s")
              .pivot("p")
              .agg(first("o")) // TODO: think about how to vectorize each entity
          if (config.verbose) {
            LOG.info(
              "Original Data DataFrame Only with numeric Literals and Pivoted:"
            )
            onlyLiteralDataWithDoubleDataFramePivoted.show(false)
          }

          onlyLiteralDataWithDoubleDataFramePivoted =
            onlyLiteralDataWithDoubleDataFramePivoted
              .toDF(
                onlyLiteralDataWithDoubleDataFramePivoted.columns.map(
                  _.replace(".", "_")
                ): _*
              )
          if (config.verbose) {
            LOG.info(
              "Original Data DataFrame Only with numeric Literals and Pivoted-Columns renamed:"
            )
            onlyLiteralDataWithDoubleDataFramePivoted.show(false)
          }
          onlyLiteralDataWithDoubleDataFramePivoted
        case config.LITERAL2FEATURE =>
          LOG.info("Starting Literal2Feature. May take time....")
          val seedVarName = "?s"
          val whereClauseForSeed = "?s ?p ?o"
          val maxUp: Int = 0
          val maxDown: Int = config.l2fDepth
          val seedNumber: Int = config.l2fSeedNumber
          val seedNumberAsRatio: Double = 1.0

          val a = createSparql(
            ds = onlyLiteralDataDataSet,
            seedVarName = seedVarName,
            seedWhereClause = whereClauseForSeed,
            maxUp = maxUp,
            maxDown = maxDown,
            numberSeeds = seedNumber,
            ratioNumberSeeds = seedNumberAsRatio
          )
          val sparqlFrame = new SparqlFrame()
            .setSparqlQuery(a._1)
            .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)
            .setCollapsByKey(false)
          var b: DataFrame = sparqlFrame.transform(onlyLiteralDataDataSet)
          b.columns.foreach(c => {
            if (!c.equals("s")) {
              b = b.withColumn(c, col(c).cast(DoubleType))
            }
          })
          b

      }

    if (config.verbose) {
      onlyLiteralDataWithDoubleDataFramePivoted.show(false)
    }
    var onlyLiteralDataWithDoubleDataFrameWithClusterId: DataFrame =
      config.clusteringMethod match {
        case config.BISECTINGKMEANS =>
          if (config.silhouetteMethod) {
            config.numberOfClusters = DistADUtil
              .detectNumberOfClusters(
                onlyLiteralDataWithDoubleDataFramePivoted,
                config.silhouetteMethodSamplingRate
              )
            LOG.info(
              "Number of optimal cluster for the dataset is " + config.numberOfClusters
            )
          }

          val predictions =
            config.clusteringType match {
              case config.PARTIAL =>
                DistADUtil.calculateBiSectingKmeanClustering(
                  onlyLiteralDataRDD,
                  config.numberOfClusters
                )
              case config.FULL =>
                DistADUtil.calculateBiSectingKmeanClustering(
                  originalDataRDD,
                  config.numberOfClusters
                )
            }

          if (config.verbose) {
            LOG.info(
              "Result of clustering with " + config.numberOfClusters + " clusters:"
            )
            predictions.show(false)
          }
          addClusterIdToData(
            onlyLiteralDataWithDoubleDataFramePivoted,
            predictions
          )

        case config.MINHASHLSH =>
          val predictions = DistADUtil.calculateMinHashLSHClustering(
            onlyLiteralDataRDD,
            originalDataRDD,
            config
          )
          if (config.verbose) {
            LOG.info(
              "Result of clustering:"
            )
            predictions.show(false)
          }
          addClusterIdToData(
            onlyLiteralDataWithDoubleDataFramePivoted,
            predictions
          )
      }

    if (config.verbose) {
      LOG.info(
        "Add clustering result to data:"
      )
      onlyLiteralDataWithDoubleDataFrameWithClusterId.show(false)
    }

    onlyLiteralDataWithDoubleDataFrameWithClusterId =
      calculateAnomaliesForMultipleFeature(
        onlyLiteralDataWithDoubleDataFrameWithClusterId
      )

    val finalExp = new StringBuilder("")
    onlyLiteralDataWithDoubleDataFrameWithClusterId.columns
      .filter(p => p.startsWith("predictedLabel"))
      .map(m => { finalExp.append(m + "==1 or ") })
    var finalExp2 = ""
    if (finalExp.nonEmpty) {
      finalExp2 = finalExp.substring(0, finalExp.length - 4)
      val cols =
        onlyLiteralDataWithDoubleDataFrameWithClusterId.columns
          .filter(p => !p.startsWith("predictedLabel"))
          .map(m => col(m))

      val result = onlyLiteralDataWithDoubleDataFrameWithClusterId
        .filter(finalExp2)
        .select(cols: _*)

      if (config.verbose) {
        result.show(false)
      }

      LOG.info("Operation took: " + (System.currentTimeMillis() - startTime))
      LOG.info("Total number of anomalies " + result.count())

      result
    } else {
      LOG.info("Operation took: " + (System.currentTimeMillis() - startTime))
      LOG.info("Total number of anomalies 0")
      spark.emptyDataFrame
    }
  }

  def calculateAnomaliesForMultipleFeature(
      data: Dataset[Row]
  ): DataFrame = {

    var originalData = data

    val data1 = data
    if (config.verbose) {
      data1.show(false)
    }

    val contamination = 0.1
    val isolationForest = new IsolationForest()
      .setNumEstimators(config.numEstimatorsForIF)
      .setBootstrap(false)
      .setMaxSamples(config.maxSampleForIF)
      .setFeaturesCol("features")
      .setPredictionCol("predictedLabel")
      .setScoreCol("outlierScore")
      .setContamination(contamination)
      .setContaminationError(0.01 * contamination)
      .setRandomSeed(1)

    var finalResult = originalData
    for (clusterId <- 0 until config.numberOfClusters) {

      var data3 = data1.filter(col("prediction") === clusterId)
      var cols =
        data3.columns.filter(!_.equals("prediction")).filter(!_.equals("s"))

      cols.foreach(col => {
        val c = data3.select(col).distinct().count()
        if (c == 1 && data3.select(col).first().isNullAt(0)) {
          data3 = data3.drop(col)
          originalData = originalData.drop(col)
          cols = cols.filter(p => !p.equals(col))
        }
      })

      val data1_1 = data3.na.fill(Double.MaxValue)
      if (config.verbose) {
        data1_1.show(false)
      }

      val vectorAssembler: VectorAssembler = new VectorAssembler()
        .setInputCols(cols)
        .setOutputCol("features")

      val data2: DataFrame = vectorAssembler.transform(data1_1)
      if (config.verbose) {
        data2.show(false)
      }

      if (!data2.isEmpty) {
        try {
          val isolationForestModel =
            isolationForest.fit(data2)
          val dataWithScores = isolationForestModel.transform(data2)
          if (config.verbose) {
            dataWithScores.show(false)
          }
          val newColName = "predictedLabel_" + clusterId
          finalResult = finalResult.join(
            dataWithScores
              .withColumnRenamed("predictedLabel", newColName)
              .select("s", newColName),
            Seq("s"),
            "leftouter"
          )
          if (config.verbose) {
            finalResult.show(false)
          }
        } catch {
          case e: Exception =>
            LOG.warn("Number of selected setMaxSamples for IF is too much")
        }
      }
    }

    finalResult
  }

  /** Adds the cluster ID to data
    * @param data
    *   data
    * @param predictions
    *   dataframe containing cluster ids
    * @return
    *   a new dataframe with cluster ids
    */
  def addClusterIdToData(
      data: DataFrame,
      predictions: DataFrame
  ): DataFrame = {
    var dataJoined =
      data.join(predictions, "s").cache()
    dataJoined = dataJoined.drop("extractedFeatures")
    dataJoined = dataJoined.drop("features")
    dataJoined = dataJoined.drop("p")
    dataJoined = dataJoined.drop("o")

    dataJoined
  }
}
