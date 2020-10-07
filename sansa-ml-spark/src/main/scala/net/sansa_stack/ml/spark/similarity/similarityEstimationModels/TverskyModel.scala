package net.sansa_stack.ml.spark.similarity.similarityEstimationModels

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, typedLit, udf}

class TverskyModel extends GenericSimilarityEstimatorModel {

  protected val tversky = udf((a: Vector, b: Vector, alpha: Double, betha: Double) => {
    val featureIndicesA = a.toSparse.indices
    val featureIndicesB = b.toSparse.indices
    val fSetA = featureIndicesA.toSet
    val fSetB = featureIndicesB.toSet
    if (fSetA.union(fSetB) == 0) {
      0.0
    }
    else {
      val tversky: Double = (
        (fSetA.intersect(fSetB).size.toDouble)/
          (
            (fSetA.intersect(fSetB).size.toDouble)
              +  (alpha * fSetA.diff(fSetB).size.toDouble)
              + (betha * fSetB.diff(fSetA).size.toDouble)
            )
        )
      tversky
    }
  })

  private var _alpha: Double = 1.0
  private var _beta: Double = 1.0

  def setAlpha(a: Double): this.type = {
    if (a < 0 || a > 1) {
      throw new Error("PROBLEM: alpha has to be between 0 and 1")
    }
    else {
      _alpha = a
      this
    }
  }

  def setBeta(b: Double): this.type = {
    if (b < 0 || b > 1) {
      throw new Error("PROBLEM: alpha has to be between 0 and 1")
    }
    else {
      _beta = b
      this
    }
  }

  override val estimatorName: String = "TverskySimilarityEstimator"
  override val estimatorMeasureType: String = "similarity"

  override val similarityEstimation = tversky

  override def similarityJoin(dfA: DataFrame, dfB: DataFrame, threshold: Double = -1.0, valueColumn: String = "tverskySimilarity"): DataFrame = {

    setSimilarityEstimationColumnName(valueColumn)

    val crossJoinDf = createCrossJoinDF(dfA: DataFrame, dfB: DataFrame)

    val join_df: DataFrame = crossJoinDf.withColumn(
      _similarityEstimationColumnName,
      similarityEstimation(col("featuresA"), col("featuresB"), lit(_alpha), lit(_beta))
    )
    reduceJoinDf(join_df, threshold)
  }

  override def nearestNeighbors(dfA: DataFrame, key: Vector, k: Int, keyUri: String = "unknown", valueColumn: String = "tverskySimilarity", keepKeyUriColumn: Boolean = false): DataFrame = {

    setSimilarityEstimationColumnName(valueColumn)

    val nnSetupDf = createNnDF(dfA, key, keyUri)

    val nnDf = nnSetupDf
      .withColumn(_similarityEstimationColumnName, similarityEstimation(col("featuresA"), col("featuresB"), lit(_alpha), lit(_beta)))

    reduceNnDf(nnDf, k, keepKeyUriColumn)
  }
}
