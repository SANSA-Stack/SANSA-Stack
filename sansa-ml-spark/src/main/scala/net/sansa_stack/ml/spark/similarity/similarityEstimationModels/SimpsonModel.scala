package net.sansa_stack.ml.spark.similarity.similarityEstimationModels

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

class SimpsonModel extends GenericSimilarityEstimatorModel {

  protected val simpson = udf( (a: Vector, b: Vector) => {
    val featureIndicesA = a.toSparse.indices
    val featureIndicesB = b.toSparse.indices
    val fSetA = featureIndicesA.toSet
    val fSetB = featureIndicesB.toSet
    val simpson = (fSetA.intersect(fSetB).size.toDouble) / Seq(fSetA.size.toDouble, fSetB.size.toDouble).min
    simpson
  })

  override val estimatorName: String = "SimpsonSimilarityEstimator"
  override val estimatorMeasureType: String = "similarity"

  override val similarityEstimation = simpson

  override def similarityJoin(dfA: DataFrame, dfB: DataFrame, threshold: Double = -1.0, valueColumn: String = "simpson_similarity"): DataFrame = {

    setSimilarityEstimationColumnName(valueColumn)

    val crossJoinDf = createCrossJoinDF(dfA: DataFrame, dfB: DataFrame)

    val joinDf: DataFrame = crossJoinDf.withColumn(
      valueColumn,
      similarityEstimation(col("featuresA"), col("featuresB"))
    )
    reduceJoinDf(joinDf, threshold)
  }

  override def nearestNeighbors(dfA: DataFrame, key: Vector, k: Int, keyUri: String = "unknown", valueColumn: String = "simpson_similarity", keepKeyUriColumn: Boolean = false): DataFrame = {

    setSimilarityEstimationColumnName(valueColumn)

    val nnSetupDf = createNnDF(dfA, key, keyUri)

    val nnDf = nnSetupDf
      .withColumn(
        valueColumn,
        similarityEstimation(col("featuresA"), col("featuresB")))

    reduceNnDf(nnDf, k, keepKeyUriColumn)
  }
}
