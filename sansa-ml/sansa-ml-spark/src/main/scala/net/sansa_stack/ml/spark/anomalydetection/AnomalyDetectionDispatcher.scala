package net.sansa_stack.ml.spark.anomalydetection

import net.sansa_stack.ml.spark.anomalydetection.DistADLogger.LOG
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.jena.graph
import org.apache.spark.sql.DataFrame

/** This class is responsible to read the config file from a path and based on
  * the values in config file initiate the right class for anomaly detection
  */
object AnomalyDetectionDispatcher {

  def main(args: Array[String]): Unit = {
    val config: DistADConfig = new DistADConfig(args(0))
    val spark = DistADUtil.createSpark()
    LOG.info(config)
    val input = config.inputData
    if (config.verbose) {
      LOG.info("Input file is: " + input)
    }
    val originalDataRDD: RDD[graph.Triple] = DistADUtil.readData(spark, input)
    if (config.verbose) {
      LOG.info("Original Data RDD:")
      originalDataRDD.take(10) foreach LOG.info
    }

    var anomalyList: DataFrame = null
    anomalyList = config.anomalyDetectionType match {
      case config.NUMERICLITERAL =>
        new NumericLiteralAnomalyDetection(spark, originalDataRDD, config)
          .run()
      case config.PREDICATE =>
        new PredicateAnomalyDetection(spark, originalDataRDD, config).run()
      case config.MULTIFEATURE =>
        new MultiFeatureAnomalyDetection(spark, originalDataRDD, config)
          .run()
      case config.CONOD =>
        new CONOD(spark, originalDataRDD, config).run()
    }

    if (config.writeResultToFile) {
      val now = System.currentTimeMillis()
      DistADUtil.writeToFile(config.resultFilePath, anomalyList)
      LOG.info(
        "writing result to file took: " + (System.currentTimeMillis() - now)
      )
    }
  }
}
