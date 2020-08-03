package net.sansa_stack.ml.spark.similarity.similarity_measures

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

class BatetModel extends GenericSimilarityEstimatorModel {

  protected val batet = udf( (a: Vector, b: Vector) => {
    val feature_indices_a = a.toSparse.indices
    val feature_indices_b = b.toSparse.indices
    val f_set_a = feature_indices_a.toSet
    val f_set_b = feature_indices_b.toSet
    var log2 = (x: Double) => scala.math.log10(x) / scala.math.log10(2.0)
    // TODO clear if subsumer is allowed to be this union instead of summing up diff diff and intersect
    val tmp: Double = log2(1.0 + ((f_set_a.diff(f_set_b).size.toDouble + f_set_b.diff(f_set_a).size.toDouble) / f_set_a.union(f_set_b).size.toDouble))
    tmp
  })

  override val estimatorName: String = "BatetSimilarityEstimator"
  override val estimatorMeasureType: String = "distance"

  override val similarityEstimation = batet

  override def similarityJoin(dfA: DataFrame, dfB: DataFrame, threshold: Double = -1.0, valueColumn: String = "batet_similarity"): DataFrame = {

    val cross_join_df = createCrossJoinDF(dfA: DataFrame, dfB: DataFrame)

    setSimilarityEstimationColumnName(valueColumn)

    val join_df: DataFrame = cross_join_df
      .withColumn(
        _similarityEstimationColumnName,
        similarityEstimation(col(_featuresColumnNameDfB), col(_featuresColumnNameDfA)))

    /* .withColumn(
    "tmp",
      similarityEstimation(col(_features_column_name_dfB), col(_features_column_name_dfA)))
    .withColumn(
      _similarity_estimation_column_name,
      log2(col("tmp"))
    ) */
    reduceJoinDf(join_df, threshold)
  }

  override def nearestNeighbors(dfA: DataFrame, key: Vector, k: Int, keyUri: String = "unknown", valueColumn: String = "batet_distance", keepKeyUriColumn: Boolean = false): DataFrame = {

    setSimilarityEstimationColumnName(valueColumn)

    val nn_setup_df = createNnDF(dfA, key, keyUri)

    val nn_df = nn_setup_df
      .withColumn(
        _similarityEstimationColumnName,
        similarityEstimation(col(_featuresColumnNameDfB), col(_featuresColumnNameDfA)))

      /* .withColumn(
      "tmp",
        similarityEstimation(col(_features_column_name_dfB), col(_features_column_name_dfA)))
      .withColumn(
        _similarity_estimation_column_name,
        log2(col("tmp"))
      ) */

    reduceNnDf(nn_df, k, keepKeyUriColumn)
  }
}
