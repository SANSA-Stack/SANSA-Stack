package net.sansa_stack.ml.spark.anomalydetection

import net.sansa_stack.ml.spark.anomalydetection.DistADLogger.LOG
import org.apache.jena.graph
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/** This class runs anomaly detection on the level of numeric literals
  * @param spark
  *   the initiated Spark session
  * @param originalDataRDD
  *   RDD[Triple]
  * @param config
  *   the config file object
  */
class NumericLiteralAnomalyDetection(
    spark: SparkSession,
    originalDataRDD: RDD[graph.Triple],
    config: DistADConfig
) {

  /** The main function
    * @return
    *   the dataframe containing all the anomalies
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

    val onlyLiteralDataDataFrame =
      DistADUtil.createDFWithConversion(onlyLiteralDataRDD).cache()
    if (config.verbose) {
      LOG.info("Original Data DataFrame Only with numeric Literals:")
      onlyLiteralDataDataFrame.show(false)
    }

    val onlyLiteralDataWithDoubleDataFrameWithClusterId: DataFrame =
      config.clusteringMethod match {
        case config.BISECTINGKMEANS =>
          if (config.silhouetteMethod) {
            config.numberOfClusters = DistADUtil
              .detectNumberOfClusters(
                onlyLiteralDataDataFrame,
                config.silhouetteMethodSamplingRate
              )
            LOG.info(
              "Number of optimal cluster for the dataset is " + config.numberOfClusters
            )
          }

          val predictions = config.clusteringType match {
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
          addClusterIdToData(onlyLiteralDataDataFrame, predictions)

        case config.MINHASHLSH =>
          DistADUtil.calculateMinHashLSHClustering(
            onlyLiteralDataRDD,
            originalDataRDD,
            config
          )

      }

    if (config.verbose) {
      LOG.info(
        "Add clustering result to data:"
      )
      onlyLiteralDataWithDoubleDataFrameWithClusterId.show(false)
    }

    val finalResult = config.anomalyDetectionAlgorithm match {
      case config.IQR =>
        DistADUtil.iqr(
          onlyLiteralDataWithDoubleDataFrameWithClusterId,
          config.verbose,
          config.anomalyListSize
        )
      case config.ZSCORE =>
        DistADUtil.zscore(
          onlyLiteralDataWithDoubleDataFrameWithClusterId,
          config.verbose,
          config.anomalyListSize
        )
      case config.MAD =>
        DistADUtil.mad(
          onlyLiteralDataWithDoubleDataFrameWithClusterId,
          config.verbose,
          config.anomalyListSize
        )
    }

    LOG.info("Operation took: " + (System.currentTimeMillis() - startTime))
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
    predictions.columns.foreach(col => {
      if (!col.equals("s") && !col.equals("prediction")) {
        dataJoined = dataJoined.drop(col)
      }
    })
    dataJoined
  }
}
