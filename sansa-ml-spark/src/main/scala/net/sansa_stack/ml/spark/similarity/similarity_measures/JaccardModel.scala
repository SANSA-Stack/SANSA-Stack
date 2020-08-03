package net.sansa_stack.ml.spark.similarity.similarity_measures

import org.apache.spark
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, lit, typedLit, udf}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

class JaccardModel extends GenericSimilarityEstimatorModel {

  protected val jaccard = udf( (a: Vector, b: Vector) => {
    val featureIndicesA = a.toSparse.indices
    val featureIndicesB = b.toSparse.indices
    val fSetA = featureIndicesA.toSet
    val fSetB = featureIndicesB.toSet
    val jaccard = fSetA.intersect(fSetB).size.toDouble / fSetA.union(fSetB).size.toDouble
    jaccard
  })

  override val estimatorName: String = "JaccardSimilarityEstimator"
  override val estimatorMeasureType: String = "similarity"

  override val similarityEstimation = jaccard

  override def similarityJoin(dfA: DataFrame, dfB: DataFrame, threshold: Double = -1.0, valueColumn: String = "jaccard_similarity"): DataFrame = {

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
