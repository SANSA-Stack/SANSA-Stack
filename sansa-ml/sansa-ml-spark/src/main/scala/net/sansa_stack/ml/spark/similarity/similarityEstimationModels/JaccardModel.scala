package net.sansa_stack.ml.spark.similarity.similarityEstimationModels

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

class JaccardModel extends GenericSimilarityEstimatorModel {

  protected val jaccard = udf( (a: Vector, b: Vector) => {
    // val featureIndicesA = a.toSparse.indices
    // val featureIndicesB = b.toSparseindices
    val fSetA = a.toSparse.indices.toSet
    val fSetB = b.toSparse.indices.toSet
    val intersection = fSetA.intersect(fSetB).size.toDouble
    val union = fSetA.union(fSetB).size.toDouble
    if (union == 0.0) {
      0
    }
    else {
      val jaccard = intersection / union
      jaccard
    }

  })

  override val estimatorName: String = "JaccardSimilarityEstimator"
  override val estimatorMeasureType: String = "similarity"

  override val similarityEstimation = jaccard

  override def similarityJoin(dfA: DataFrame, dfB: DataFrame, threshold: Double = -1.0, valueColumn: String = "jaccardSimilarity"): DataFrame = {

    setSimilarityEstimationColumnName(valueColumn)

    val crossJoinDf = createCrossJoinDF(dfA: DataFrame, dfB: DataFrame)

    val joinDf: DataFrame = crossJoinDf.withColumn(
      valueColumn,
      similarityEstimation(col("featuresA"), col("featuresB"))
    )
    reduceJoinDf(joinDf, threshold)
  }

  override def nearestNeighbors(dfA: DataFrame, key: Vector, k: Int, keyUri: String = "unknown", valueColumn: String = "jaccardSimilarity", keepKeyUriColumn: Boolean = false): DataFrame = {

    setSimilarityEstimationColumnName(valueColumn)

    val nnSetupDf = createNnDF(dfA, key, keyUri)

    val nnDf = nnSetupDf
      .withColumn(
        valueColumn,
        similarityEstimation(col("featuresA"), col("featuresB")))

    reduceNnDf(nnDf, k, keepKeyUriColumn)
  }
}
