package net.sansa_stack.ml.spark.anomalydetection

import net.sansa_stack.ml.spark.anomalydetection.DistADLogger.LOG
import org.apache.jena.graph
import org.apache.jena.graph.{Node, Node_Graph, Node_Literal, Node_URI}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, count, expr, lit}

/** This class runs anomaly detection on the level of predicates
  * @param spark
  *   the initiated Spark session
  * @param originalDataRDD
  *   RDD[Triple]
  * @param config
  *   the config file object
  */
class PredicateAnomalyDetection(
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

    val originalDataDataFrame =
      DistADUtil.createDF(originalDataRDD).cache()
    if (config.verbose) {
      LOG.info("Original Data DataFrame:")
      originalDataDataFrame.show(false)
    }

    val predefinedP: List[String] = List()

    var originalDataDataFrameCounted: DataFrame = null
    if (predefinedP.isEmpty) {
      originalDataDataFrameCounted = originalDataDataFrame
        .groupBy("s", "p")
        .agg(count("p") as "o")
    } else {
      originalDataDataFrameCounted = originalDataDataFrame
        .filter(p => {
          if (predefinedP.nonEmpty) {
            predefinedP.contains(p.getAs[String](1))
          } else {
            true
          }
        })
        .groupBy("s", "p")
        .agg(count("p") as "o")
    }

    if (config.verbose) {
      LOG.info(
        "Result of counting dataframe:"
      )
      originalDataDataFrameCounted.show(false)
    }

    implicit val tripleTupleEncoder = Encoders.kryo(classOf[graph.Triple])
    val originalDataRDDCounted: RDD[graph.Triple] = originalDataDataFrameCounted
      .map(row => {
        val s: graph.Node = graph.NodeFactory.createURI(row.get(0).toString)
        val p: graph.Node = graph.NodeFactory.createURI(row.get(1).toString)
        val o: graph.Node =
          graph.NodeFactory.createLiteral(
            row.get(2).toString + "^^<http://www.w3.org/2001/XMLSchema#integer>"
          )
        graph.Triple.create(s, p, o)
      })
      .rdd

    val originalDataDataFrameCountedWithClusterId: DataFrame =
      config.clusteringMethod match {
        case config.BISECTINGKMEANS =>
          if (config.silhouetteMethod) {
            config.numberOfClusters = DistADUtil
              .detectNumberOfClusters(
                originalDataDataFrameCounted,
                config.silhouetteMethodSamplingRate
              )
            LOG.info(
              "Number of optimal cluster for the dataset is " + config.numberOfClusters
            )
          }

          val predictions = config.clusteringType match {
            case config.PARTIAL =>
              throw new Exception(
                "Partial mode is not available for predicates"
              )
            case config.FULL =>
              DistADUtil.calculateBiSectingKmeanClustering(
                originalDataRDDCounted,
                config.numberOfClusters
              )
          }

          if (config.verbose) {
            LOG.info(
              "Result of clustering with " + config.numberOfClusters + " clusters:"
            )
            predictions.show(false)
          }
          addClusterIdToData(originalDataDataFrameCounted, predictions)

        case config.MINHASHLSH =>
          DistADUtil.calculateMinHashLSHClustering(
            originalDataRDDCounted,
            originalDataRDDCounted,
            config
          )
      }

    if (config.verbose) {
      LOG.info(
        "Add clustering result to data:"
      )
      originalDataDataFrameCountedWithClusterId.show(false)
    }

    val finalResult = config.anomalyDetectionAlgorithm match {
      case config.IQR =>
        DistADUtil.iqr(
          originalDataDataFrameCountedWithClusterId,
          config.verbose,
          config.anomalyListSize
        )
      case config.ZSCORE =>
        DistADUtil.zscore(
          originalDataDataFrameCountedWithClusterId,
          config.verbose,
          config.anomalyListSize
        )
      case config.MAD =>
        DistADUtil.mad(
          originalDataDataFrameCountedWithClusterId,
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
