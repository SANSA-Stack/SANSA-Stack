package net.sansa_stack.ml.spark.similarity.similarity_measures

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, typedLit, udf}

class TverskyModel extends GenericSimilarityEstimatorModel {

  protected val tversky = udf((a: Vector, b: Vector, alpha: Double, betha: Double) => {
    val feature_indices_a = a.toSparse.indices
    val feature_indices_b = b.toSparse.indices
    val f_set_a = feature_indices_a.toSet
    val f_set_b = feature_indices_b.toSet
    /* println(f_set_a)
    println(f_set_b)
    val c: Double = (f_set_a.intersect(f_set_b).size.toDouble)
    val e1: Double = (f_set_a.intersect(f_set_b).size.toDouble)
    val e2: Double = (alpha * f_set_a.diff(f_set_b).size.toDouble)
    val e3: Double = (betha * f_set_b.diff(f_set_a).size.toDouble)
    val e: Double = e1 + e2 + e3
    println(alpha, betha, c, e1, e2, e3, e)
    assert(e > 0)
    val tversky = c/e */
    val tversky: Double = (
      (f_set_a.intersect(f_set_b).size.toDouble)/
        (
          (f_set_a.intersect(f_set_b).size.toDouble)
          +  (alpha * f_set_a.diff(f_set_b).size.toDouble)
          + (betha * f_set_b.diff(f_set_a).size.toDouble)
        )
    )

    tversky
  })

  private var _alpha: Double = 1.0
  private var _betha: Double = 1.0

  def set_alpha(a: Double): this.type = {
    if (a < 0 || a > 1) {
      throw new Error("PROBLEM: alpha has to be between 0 and 1")
    }
    else {
      _alpha = a
      this
    }
  }

  def set_betha(b: Double): this.type = {
    if (b < 0 || b > 1) {
      throw new Error("PROBLEM: alpha has to be between 0 and 1")
    }
    else {
      _betha = b
      this
    }
  }

  override val estimatorName: String = "TverskySimilarityEstimator"
  override val estimatorMeasureType: String = "similarity"

  override val similarityEstimation = tversky

  override def similarityJoin(dfA: DataFrame, dfB: DataFrame, threshold: Double = -1.0, valueColumn: String = "tversky_similarity"): DataFrame = {

    checkColumnNames(dfA, dfB)

    val cross_join_df = createCrossJoinDF(dfA: DataFrame, dfB: DataFrame)

    setSimilarityEstimationColumnName(valueColumn)

    val join_df: DataFrame = cross_join_df.withColumn(
      _similarityEstimationColumnName,
      similarityEstimation(col(_featuresColumnNameDfA), col(_featuresColumnNameDfB), lit(_alpha), lit(_betha))
    )
    reduceJoinDf(join_df, threshold)
  }

  override def nearestNeighbors(dfA: DataFrame, key: Vector, k: Int, keyUri: String = "unknown", valueColumn: String = "tversky_similarity", keepKeyUriColumn: Boolean = false): DataFrame = {

    setSimilarityEstimationColumnName(valueColumn)

    val nn_setup_df = createNnDF(dfA, key, keyUri)

    val nn_df = nn_setup_df
      .withColumn(_similarityEstimationColumnName, similarityEstimation(col(_featuresColumnNameDfB), col(_featuresColumnNameDfA), lit(_alpha), lit(_betha)))

    reduceNnDf(nn_df, k, keepKeyUriColumn)
  }
}
