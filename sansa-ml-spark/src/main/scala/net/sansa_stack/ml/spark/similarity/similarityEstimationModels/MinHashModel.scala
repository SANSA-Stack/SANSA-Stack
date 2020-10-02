package net.sansa_stack.ml.spark.similarity.similarityEstimationModels

import org.apache.spark.ml.feature.{MinHashLSH, MinHashLSHModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf}

class MinHashModel extends GenericSimilarityEstimatorModel {

  override val estimatorName: String = "MinHashLSHSimilarityEstimator"
  override val estimatorMeasureType: String = "distance"

  private var numberHashTables: Int = 1

  def setNumHashTables(n: Int): this.type = {
    numberHashTables = n
    this
  }

  override def similarityJoin(dfA: DataFrame, dfB: DataFrame, threshold: Double = -1.0, valueColumn: String = "distCol"): DataFrame = {

    val minHashModel: MinHashLSHModel = new MinHashLSH()
      .setInputCol("vectorizedFeatures")
      .setOutputCol("hashedFeatures")
      .fit(dfA)
    // minHashModel.approxNearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, "minHashDistance").show()
    minHashModel
      .approxSimilarityJoin(dfA, dfB, threshold, valueColumn)
      .withColumn("uri_A", col("datasetA").getField("uri"))
      .withColumn("uri_B", col("datasetB").getField("uri"))
      .select("uri_A", "uri_B", valueColumn)
  }

  override def nearestNeighbors(dfA: DataFrame, key: Vector, k: Int, keyUri: String = "unknown", valueColumn: String = "jaccardSimilarity", keepKeyUriColumn: Boolean = false): DataFrame = {

    val minHashModel: MinHashLSHModel = new MinHashLSH()
      .setNumHashTables(numberHashTables)
      .setInputCol("vectorizedFeatures")
      .setOutputCol("hashedFeatures")
      .fit(dfA)
    // minHashModel.approxNearestNeighbors(countVectorizedFeaturesDataFrame, sample_key, 10, "minHashDistance").show()
    minHashModel
      .approxNearestNeighbors(dfA, key, k, valueColumn)
      .withColumn("key_column", lit("key_uri"))
      .select("key_column", "uri", valueColumn)
  }
}
