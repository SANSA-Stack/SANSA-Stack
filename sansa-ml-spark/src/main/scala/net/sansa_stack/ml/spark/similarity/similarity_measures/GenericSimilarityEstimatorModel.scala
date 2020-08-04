package net.sansa_stack.ml.spark.similarity.similarity_measures

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.{col, udf, lit, typedLit}
import org.apache.spark.sql.{DataFrame, SparkSession}


class GenericSimilarityEstimatorModel {

  protected var _uriColumnNameDfA: String = "uri"
  protected var _featuresColumnNameDfA: String = "vectorizedFeatures"
  protected var _uriColumnNameDfB: String = "uri"
  protected var _featuresColumnNameDfB: String = "vectorizedFeatures"

  protected var _inputCol = "vectorizedFeatures"

  // values that have to be overwritten
  protected var _similarityEstimationColumnName = "estimation"

  val estimatorName: String = "GenericSimilarityEstimator"
  val estimatorMeasureType: String = "distance, related or similarity"

  protected val similarityEstimation = udf( (a: Vector, b: Vector) => {
    throw new Exception("this function should not be called")
  })
  def setInputCol(inputCol: String): this.type = {
    _inputCol = inputCol
    _featuresColumnNameDfA = inputCol
    _featuresColumnNameDfB = inputCol
    this
  }

  def setUriColumnNameDfA(uri_column_name: String): this.type = {
    _uriColumnNameDfA = uri_column_name
    this
  }

  def setUriColumnNameDfB(uri_column_name: String): this.type = {
    _uriColumnNameDfB = uri_column_name
    this
  }

  def setFeaturesColumnNameDfA(features_column_name: String): this.type = {
    _featuresColumnNameDfA = features_column_name
    this
  }

  def setFeaturesColumnNameDfB(features_column_name: String): this.type = {
    _featuresColumnNameDfB = features_column_name
    this
  }

  protected def checkColumnNames(dfA: DataFrame, dfB: DataFrame): Unit = {
    // check if column names are set right or if setter method are used right way
    if (dfA.columns.contains(_uriColumnNameDfA) == false) throw new Exception("current set uri column name: " + _uriColumnNameDfA + "does not align with the column names of given DataFrameA. please set column name with set_uri_column_name_dfA()!")
    if (dfA.columns.contains(_uriColumnNameDfB) == false) throw new Exception("current set uri column name: " + _uriColumnNameDfB + "does not align with the column names of given DataFrameB. please set column name with set_uri_column_name_dfB()!")
    if (dfA.columns.contains(_featuresColumnNameDfA) == false) throw new Exception("current set features column name: " + _featuresColumnNameDfA + "does not align with the column names of given DataFrameA. please set column name with set_features_column_name_dfA()!")
    if (dfA.columns.contains(_featuresColumnNameDfA) == false) throw new Exception("current set features column name: " + _featuresColumnNameDfA + "does not align with the column names of given DataFrameA. please set column name with set_features_column_name_dfB()!")
  }

  protected def createCrossJoinDF(dfA: DataFrame, dfB: DataFrame): DataFrame = {

    val crossJoinDf: DataFrame =
      dfA
        .withColumnRenamed(_uriColumnNameDfA, "uriA")
        .withColumnRenamed(_featuresColumnNameDfA, "featuresA")
        .crossJoin(
          dfB
            .withColumnRenamed(_uriColumnNameDfB, "uriB")
            .withColumnRenamed(_featuresColumnNameDfB, "featuresB"))

    /* _uriColumnNameDfA = newUriColumnA
    _uriColumnNameDfB = newUriColumnB
    _featuresColumnNameDfA = newFeaturesColumnA
    _featuresColumnNameDfB = newFeaturesColumnB */

    crossJoinDf
  }

  protected def createNnDF(df: DataFrame, key: Vector, keyUri: String = "unknown"): DataFrame = {
    var uri = keyUri
    if (keyUri == "unknown") uri = "genericKeyUri" + key.toString

    df
      .withColumnRenamed(_uriColumnNameDfA, "uriA")
      .withColumnRenamed(_featuresColumnNameDfA, "featuresA")
      .withColumn("uriB", lit(uri))
      .withColumn("featuresB", typedLit(key))
      .select( "uriA", "uriB", "featuresA", "featuresB")
  }

  protected def setSimilarityEstimationColumnName(valueColumnName: String): Unit = {
    _similarityEstimationColumnName = valueColumnName
  }

  protected def reduceJoinDf(simDf: DataFrame, threshold: Double): DataFrame = {
    val tmpDf = simDf.select("uriA", "uriB", _similarityEstimationColumnName)

    if (threshold == -1.0) {
      // -1 as inicator for no filtering
      tmpDf
    }
    else {
      val filtered_df = if (estimatorMeasureType == "distance") {
        tmpDf.filter(col(_similarityEstimationColumnName) <= threshold)
      }
      else {
        tmpDf.filter(col(_similarityEstimationColumnName) >= threshold)
      }
      filtered_df
    }
  }

  protected def reduceNnDf(simDf: DataFrame, k: Int, keepKeyUriColumn: Boolean): DataFrame = {
    val tmpDf = if (keepKeyUriColumn) simDf.select("uriA", "uriB", _similarityEstimationColumnName) else simDf.select("uriA", _similarityEstimationColumnName)
    val orderedDf = if (estimatorMeasureType == "distance") tmpDf.orderBy(col(_similarityEstimationColumnName).asc) else tmpDf.orderBy(col(_similarityEstimationColumnName).desc)
    val clippedDf: DataFrame = orderedDf.limit(k)
    clippedDf
  }

  def similarityJoin(dfA: DataFrame, dfB: DataFrame, threshold: Double = -1.0, valueColumn: String): DataFrame = {

    setSimilarityEstimationColumnName(valueColumn)

    val crossJoinDf = createCrossJoinDF(dfA: DataFrame, dfB: DataFrame)

    val joinDf: DataFrame = crossJoinDf.withColumn(
      valueColumn,
      similarityEstimation(col("featuresA"), col("featuresB"))
    )
    reduceJoinDf(joinDf, threshold)
  }

  def nearestNeighbors(dfA: DataFrame, key: Vector, k: Int, keyUri: String, valueColumn: String, keepKeyUriColumn: Boolean = false): DataFrame = {

    setSimilarityEstimationColumnName(valueColumn)

    val nnSetupDf = createNnDF(dfA, key, keyUri)

    val nnDf = nnSetupDf
      .withColumn(
        valueColumn,
        similarityEstimation(col("featuresA"), col("featuresB")))

    reduceNnDf(nnDf, k, keepKeyUriColumn)
  }
}
