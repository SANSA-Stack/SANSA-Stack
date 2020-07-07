package net.sansa_stack.ml.spark.similarity.similarity_measures

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf, lit, typedLit}


class JaccardModel {

  private var _uri_column_name_dfA: String = _
  private var _features_column_name_dfA: String = _
  private var _uri_column_name_dfB: String = _
  private var _features_column_name_dfB: String = _

  val jaccard = udf( (a: Vector, b: Vector) => {
    val feature_indices_a = a.toSparse.indices
    val feature_indices_b = b.toSparse.indices
    val f_set_a = feature_indices_a.toSet
    val f_set_b = feature_indices_b.toSet
    val jaccard = f_set_a.intersect(f_set_b).size.toDouble / f_set_a.union(f_set_b).size.toDouble
    jaccard
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

  def similarityJoin(df_A: DataFrame, df_B: DataFrame, min_similarity_threshold: Double, value_column: String): DataFrame = {

    var new_uri_column_A: String = _uri_column_name_dfA
    var new_uri_column_B: String = _uri_column_name_dfB

    if (new_uri_column_A == new_uri_column_B) {
      new_uri_column_A = new_uri_column_A + "_A"
      new_uri_column_B = new_uri_column_B + "_B"
    }

    val new_features_column_A: String = _features_column_name_dfA + "_A"
    val new_features_column_B: String = _features_column_name_dfB + "_B"

    val cross_for_jaccard: DataFrame =
      df_A
        .withColumnRenamed(_uri_column_name_dfA, new_uri_column_A)
        .withColumnRenamed(_features_column_name_dfA, new_features_column_A)
        .crossJoin(
          df_B
            .withColumnRenamed(_uri_column_name_dfB, new_uri_column_B)
            .withColumnRenamed(_features_column_name_dfB, new_features_column_B))
    val jaccard_eval_df: DataFrame = cross_for_jaccard.withColumn(
      value_column,
      jaccard(col(new_features_column_A), col(new_features_column_B))
    )
      .select(new_uri_column_A, new_uri_column_B, value_column)
    jaccard_eval_df
  }

  def nearestNeighbors(dfA: DataFrame, key: Vector, k: Int, key_uri: String = "generic_key_uri", value_column: String = "jaccard_similarity"): DataFrame = {
    var uri = key_uri
    if (uri == "generic_key_uri") uri = uri + key.toString

    val key_uri_column_name: String = "key_uri"
    val key_feature_vector_column_name: String = "key_feature_vector"

    val tmp_df = dfA
      .withColumn(key_uri_column_name, lit(key_uri))
      .withColumn(key_feature_vector_column_name, typedLit(key))
    val nn_df = tmp_df
      .withColumn(value_column, jaccard(col(key_feature_vector_column_name), col(_features_column_name_dfA)))
      .select(_uri_column_name_dfA, value_column)
    nn_df
  }
}
