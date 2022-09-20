package net.sansa_stack.ml.spark.explainableanomalydetection

import com.typesafe.config.{Config, ConfigFactory}
import net.sansa_stack.ml.spark.utils.ConfigResolver

/** This class gets a config file path and reads the values from the config file
  *
  * @param path the path of the config file
  */
class ExPADConfig(path: String) extends Serializable {
  val cfg: Config = new ConfigResolver(path).getConfig()

  /**
    * Static values
    */
  val IQR: String = "IQR"
  val MAD: String = "MAD"
  val ZSCORE: String = "ZSCORE"
  val PIVOT: String = "Pivot"
  val LITERAL2FEATURE: String = "Literal2Feature"

  /**
    * Configurable values
    */
  var verbose: Boolean =
    if (cfg.hasPath("verbose"))
      cfg.getBoolean("verbose")
    else
      false

  var writeResultToFile: Boolean =
    if (cfg.hasPath("writeResultToFile"))
      cfg.getBoolean("writeResultToFile")
    else
      false
  var inputData: String =
    if (cfg.hasPath("inputData")) cfg.getString("inputData") else ""
  var resultFilePath: String =
    if (cfg.hasPath("resultFilePath")) cfg.getString("resultFilePath") else ""
  var anomalyListSize: Int =
    if (cfg.hasPath("anomalyListSize")) cfg.getInt("anomalyListSize") else 1
  var anomalyDetectionAlgorithm: String =
    if (cfg.hasPath("anomalyDetectionAlgorithm"))
      cfg.getString("anomalyDetectionAlgorithm")
    else "IQR"
  var featureExtractor: String =
    if (cfg.hasPath("featureExtractor")) cfg.getString("featureExtractor")
    else "Pivot"

  /**
    * Literal2Feature
    */
  var l2fDepth: Int = if (cfg.hasPath("depth")) cfg.getInt("depth") else 1
  var l2fSeedNumber: Int =
    if (cfg.hasPath("seedNumber")) cfg.getInt("seedNumber") else 1

  /**
    * Decision Tree
    */
  var maxDepth: Int = if (cfg.hasPath("maxDepth")) cfg.getInt("maxDepth") else 2
  var maxBin: Int = if (cfg.hasPath("maxBin")) cfg.getInt("maxBin") else 5

  override def toString: String =
    s"ExPADConfig(\nverbose=$verbose,\nwriteResultToFile=$writeResultToFile,\ninputData=$inputData,\nresultFilePath=$resultFilePath,\nanomalyListSize=$anomalyListSize,\nanomalyDetectionAlgorithm=$anomalyDetectionAlgorithm,\nfeatureExtractor=$featureExtractor,\nl2fDepth=$l2fDepth,\nl2fSeedNumber=$l2fSeedNumber,\nmaxDepth=$maxDepth,\nmaxBin=$maxBin\n)"
}
