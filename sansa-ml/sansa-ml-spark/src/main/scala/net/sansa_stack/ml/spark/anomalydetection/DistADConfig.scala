package net.sansa_stack.ml.spark.anomalydetection

import com.typesafe.config.Config
import net.sansa_stack.ml.spark.utils.ConfigResolver

/** This class gets a config file path and reads the values from the config file
  * @param path the path of the config file
  */
class DistADConfig(path: String) extends Serializable {
  val cfg: Config = new ConfigResolver(path).getConfig()

  /**
    * Static values
    */
  val PARTIAL: String = "Partial"
  val FULL: String = "Full"
  val MINHASHLSH: String = "MinHashLSH"
  val BISECTINGKMEANS: String = "BisectingKmeans"
  val IQR: String = "IQR"
  val MAD: String = "MAD"
  val ZSCORE: String = "ZSCORE"
  val PIVOT: String = "Pivot"
  val LITERAL2FEATURE: String = "Literal2Feature"
  val NUMERICLITERAL: String = "NumericLiteral"
  val PREDICATE: String = "Predicate"
  val MULTIFEATURE: String = "MultiFeature"
  val CONOD: String = "CONOD"

  var silhouetteMethod: Boolean = cfg.getBoolean("silhouetteMethod")
  var silhouetteMethodSamplingRate: Double =
    cfg.getDouble("silhouetteMethodSamplingRate")
  var verbose: Boolean = cfg.getBoolean("verbose")
  var writeResultToFile: Boolean = cfg.getBoolean("writeResultToFile")
  var clusteringType: String = cfg.getString("clusteringType")
  var inputData: String = cfg.getString("inputData")
  var resultFilePath: String = cfg.getString("resultFilePath")
  var anomalyListSize: Int = cfg.getInt("anomalyListSize")
  var anomalyDetectionType: String = cfg.getString("anomalyDetectionType")
  var anomalyDetectionAlgorithm: String =
    cfg.getString("anomalyDetectionAlgorithm")
  var featureExtractor: String = cfg.getString("featureExtractor")
  var numberOfClusters: Int = cfg.getInt("numberOfClusters")

  var clusteringMethod: String = cfg.getString("clusteringMethod")

  /** CONOD
    */
  var pairWiseDistanceThreshold: Double =
    cfg.getDouble("pairWiseDistanceThreshold")

  /** Isolation Forest
    */
  var maxSampleForIF: Int = cfg.getInt("maxSampleForIF")
  var numEstimatorsForIF: Int = cfg.getInt("numEstimatorsForIF")

  /**
    * Literal2Feature
    */
  var l2fDepth: Int = cfg.getInt("depth")
  var l2fSeedNumber: Int = cfg.getInt("seedNumber")

  override def toString =
    s"DistADConfig(silhouetteMethod=$silhouetteMethod, silhouetteMethodSamplingRate=$silhouetteMethodSamplingRate, verbose=$verbose, writeResultToFile=$writeResultToFile, clusteringType=$clusteringType, inputData=$inputData, resultFilePath=$resultFilePath, anomalyListSize=$anomalyListSize, anomalyDetectionType=$anomalyDetectionType, anomalyDetectionAlgorithm=$anomalyDetectionAlgorithm, featureExtractor=$featureExtractor, numberOfClusters=$numberOfClusters, clusteringMethod=$clusteringMethod, pairWiseDistanceThreshold=$pairWiseDistanceThreshold, maxSampleForIF=$maxSampleForIF, numEstimatorsForIF=$numEstimatorsForIF, l2fDepth=$l2fDepth, l2fSeedNumber=$l2fSeedNumber)"
}
