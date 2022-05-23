package net.sansa_stack.ml.spark.explainableanomalydetection

import com.typesafe.config.Config
import net.sansa_stack.ml.spark.utils.ConfigResolver

/** This class gets a config file path and reads the values from the config file
  *
  * @param path the path of the config file
  */
class ExDistADConfig(path: String) extends Serializable {
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
  var verbose: Boolean = cfg.getBoolean("verbose")
  var writeResultToFile: Boolean = cfg.getBoolean("writeResultToFile")
  var inputData: String = cfg.getString("inputData")
  var resultFilePath: String = cfg.getString("resultFilePath")
  var anomalyListSize: Int = cfg.getInt("anomalyListSize")
  var anomalyDetectionAlgorithm: String =
    cfg.getString("anomalyDetectionAlgorithm")
  var featureExtractor: String = cfg.getString("featureExtractor")

  /**
    * Literal2Feature
    */
  var l2fDepth: Int = cfg.getInt("depth")
  var l2fSeedNumber: Int = cfg.getInt("seedNumber")

  /**
    * Decision Tree
    */
  var maxDepth: Int = cfg.getInt("maxDepth")
  var maxBin: Int = cfg.getInt("maxBin")

  override def toString: String =
    s"ExDistADConfig(\nverbose=$verbose,\nwriteResultToFile=$writeResultToFile,\ninputData=$inputData,\nresultFilePath=$resultFilePath,\nanomalyListSize=$anomalyListSize,\nanomalyDetectionAlgorithm=$anomalyDetectionAlgorithm,\nfeatureExtractor=$featureExtractor,\nl2fDepth=$l2fDepth,\nl2fSeedNumber=$l2fSeedNumber,\nmaxDepth=$maxDepth,\nmaxBin=$maxBin\n)"
}
