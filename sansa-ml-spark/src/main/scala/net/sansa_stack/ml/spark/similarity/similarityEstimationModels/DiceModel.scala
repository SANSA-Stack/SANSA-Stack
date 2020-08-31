package net.sansa_stack.ml.spark.similarity.similarityEstimationModels

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, typedLit, udf}

class DiceModel extends GenericSimilarityEstimatorModel {

  protected val dice = udf( (a: Vector, b: Vector) => {
    val featureIndicesA = a.toSparse.indices
    val featureIndicesB = b.toSparse.indices
    val fSetA = featureIndicesA.toSet
    val fSetB = featureIndicesB.toSet
    val dice = (2 * fSetA.intersect(fSetB).size.toDouble) / (fSetA.size.toDouble + fSetB.size.toDouble)
    dice
  })

  override val estimatorName: String = "DiceSimilarityEstimator"
  override val estimatorMeasureType: String = "similarity"

  override val similarityEstimation = dice

  override def similarityJoin(dfA: DataFrame, dfB: DataFrame, threshold: Double = -1.0, valueColumn: String = "dice_similarity"): DataFrame = {

    setSimilarityEstimationColumnName(valueColumn)

    val crossJoinDf = createCrossJoinDF(dfA: DataFrame, dfB: DataFrame)

    val joinDf: DataFrame = crossJoinDf.withColumn(
      valueColumn,
      similarityEstimation(col("featuresA"), col("featuresB"))
    )
    reduceJoinDf(joinDf, threshold)
  }

  override def nearestNeighbors(dfA: DataFrame, key: Vector, k: Int, keyUri: String = "unknown", valueColumn: String = "dice_similarity", keepKeyUriColumn: Boolean = false): DataFrame = {

    setSimilarityEstimationColumnName(valueColumn)

    val nnSetupDf = createNnDF(dfA, key, keyUri)

    val nnDf = nnSetupDf
      .withColumn(
        valueColumn,
        similarityEstimation(col("featuresA"), col("featuresB")))

    reduceNnDf(nnDf, k, keepKeyUriColumn)
  }
}