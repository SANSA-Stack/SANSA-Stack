package net.sansa_stack.ml.spark.similarity.similarity_measures

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.{col, udf, lit, typedLit}
import org.apache.spark.sql.{DataFrame, SparkSession}


class GenericSimilarityEstimatorModel {

  protected var _uri_column_name_dfA: String = _
  protected var _features_column_name_dfA: String = _
  protected var _uri_column_name_dfB: String = _
  protected var _features_column_name_dfB: String = _

  // values that have to be overwritten
  protected var _similarity_estimation_column_name = "genericColumnName"

  val estimator_name: String = "GenericSimilarityEstimator"
  val estimator_measure_type: String = "distance, related or similarity"

  protected val similarityEstimation = udf( (a: Vector, b: Vector) => {
    throw new Exception("this function should not be called")
  })

  def set_uri_column_name_dfA(uri_column_name: String): Unit = {
    _uri_column_name_dfA = uri_column_name
  }

  def set_uri_column_name_dfB(uri_column_name: String): Unit = {
    _uri_column_name_dfB = uri_column_name
  }

  def set_features_column_name_dfA(features_column_name: String): Unit = {
    _features_column_name_dfA = features_column_name
  }

  def set_features_column_name_dfB(features_column_name: String): Unit = {
    _features_column_name_dfB = features_column_name
  }

  protected def createCrossJoinDF(df_A: DataFrame, df_B: DataFrame): DataFrame = {

    var new_uri_column_A: String = _uri_column_name_dfA
    var new_uri_column_B: String = _uri_column_name_dfB

    if (new_uri_column_A == new_uri_column_B) {
      new_uri_column_A = new_uri_column_A + "_A"
      new_uri_column_B = new_uri_column_B + "_B"
      println("WARNING: names column names were changed because they were the same so added _A and _B, if you want to call nearest neighbors reinitialize!")
    }

    val new_features_column_A: String = _features_column_name_dfA + "_A"
    val new_features_column_B: String = _features_column_name_dfB + "_B"

    val cross_join_df: DataFrame =
      df_A
        .withColumnRenamed(_uri_column_name_dfA, new_uri_column_A)
        .withColumnRenamed(_features_column_name_dfA, new_features_column_A)
        .crossJoin(
          df_B
            .withColumnRenamed(_uri_column_name_dfB, new_uri_column_B)
            .withColumnRenamed(_features_column_name_dfB, new_features_column_B))

    _uri_column_name_dfA = new_uri_column_A
    _uri_column_name_dfB = new_uri_column_B
    _features_column_name_dfA = new_features_column_A
    _features_column_name_dfB = new_features_column_B

    cross_join_df
  }

  protected def createNnDF(df: DataFrame, key: Vector, key_uri: String): DataFrame = {
    var uri = key_uri
    if (key_uri == "generic_key_uri") uri = uri + key.toString

    _uri_column_name_dfB = "key_uri"
    _features_column_name_dfB = "key_feature_vector"

    df
      .withColumn(_uri_column_name_dfB, lit(key_uri))
      .withColumn(_features_column_name_dfB, typedLit(key))
      .select(_uri_column_name_dfB, _uri_column_name_dfA, _features_column_name_dfB, _features_column_name_dfA)
  }

  protected def set_similarity_estimation_column_name(value_column_name: String): Unit = {
    _similarity_estimation_column_name = value_column_name
  }

  protected def reduce_join_df(sim_df: DataFrame, threshold: Double): DataFrame = {
    val tmp_df = sim_df.select(_uri_column_name_dfA, _uri_column_name_dfB, _similarity_estimation_column_name)

    if (threshold == -1.0) {
      tmp_df
    }
    else {
      val filtered_df = if (estimator_measure_type == "distance") tmp_df.filter(col(_similarity_estimation_column_name) <= threshold) else tmp_df.filter(col(_similarity_estimation_column_name) >= threshold)
      filtered_df
    }
  }

  protected def reduce_nn_df(sim_df: DataFrame, k: Int, keep_key_uri_column: Boolean): DataFrame = {
    val tmp_df = if (keep_key_uri_column) sim_df.select(_uri_column_name_dfB, _uri_column_name_dfA, _similarity_estimation_column_name) else sim_df.select(_uri_column_name_dfA, _similarity_estimation_column_name)
    val ordered_df = if (estimator_measure_type == "distance") tmp_df.orderBy(col(_similarity_estimation_column_name).asc) else tmp_df.orderBy(col(_similarity_estimation_column_name).desc)
    val clipped_df: DataFrame = ordered_df.limit(k)
    clipped_df
  }

  def similarityJoin(df_A: DataFrame, df_B: DataFrame, threshold: Double = -1.0, value_column: String = "generic_similarity"): DataFrame = {

    val cross_join_df = createCrossJoinDF(df_A: DataFrame, df_B: DataFrame)

    set_similarity_estimation_column_name(value_column)

    val join_df: DataFrame = cross_join_df.withColumn(
      _similarity_estimation_column_name,
      similarityEstimation(col(_features_column_name_dfA), col(_features_column_name_dfB))
    )
    reduce_join_df(join_df, threshold)
  }

  def nearestNeighbors(df_A: DataFrame, key: Vector, k: Int, key_uri: String = "generic_key_uri", value_column: String = "generic_similarity", keep_key_uri_column: Boolean = false): DataFrame = {

    set_similarity_estimation_column_name(value_column)

    val nn_setup_df = createNnDF(df_A, key, key_uri)
    val nn_df = nn_setup_df
      .withColumn(_similarity_estimation_column_name, similarityEstimation(col(_features_column_name_dfB), col(_features_column_name_dfA)))

    reduce_nn_df(nn_df, k, keep_key_uri_column)
  }
}
