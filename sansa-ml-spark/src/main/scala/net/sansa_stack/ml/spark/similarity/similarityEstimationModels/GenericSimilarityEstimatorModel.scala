package net.sansa_stack.ml.spark.similarity.similarityEstimationModels

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.{col, udf, lit, typedLit}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This class is the superclass for all novel created feature based semantic similarity models
 *
 * it provides a set of methods and mainly shouws how needed overwritten functions should look like
 */
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

  /**
   * This method creates a cross join dataframe with all possible pairs in the dataframe which can be compared
   * @param dfA is the first dataFrame which has to have column for URI and for the Vector based feature vector
   * @param dfB is second dataframe
   * @return it return a dataframe with four columns two for the uri columns and two for the feature vector. the size is approx the product of both dataframe sizes
   */
  protected def createCrossJoinDF(dfA: DataFrame, dfB: DataFrame): DataFrame = {

    val crossJoinDf: DataFrame =
      dfA
        .withColumnRenamed(_uriColumnNameDfA, "uriA")
        .withColumnRenamed(_featuresColumnNameDfA, "featuresA")
        .crossJoin(
          dfB
            .withColumnRenamed(_uriColumnNameDfB, "uriB")
            .withColumnRenamed(_featuresColumnNameDfB, "featuresB"))


    crossJoinDf
  }

  /**
   * This method creates a dataframe aligned to createCrossJoinDF result. but in this case we only assign to each
   * @param df the dataframe with all entities we want to compare against with all uri feature vector representations
   * @param key the vector representation of uri features
   * @param keyUri you can specify the key uri name
   * @return dataframe with four columns with all desired pairs to compare. uriA, uriB, featuresA, featuresB
   */
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

  /**
   * This method sets the column name for the resulting dataframe similarity or distance: e.g. "jaccardSimilarity"
   * @param valueColumnName the name of the resulting distance/similarity value
   */
  protected def setSimilarityEstimationColumnName(valueColumnName: String): Unit = {
    _similarityEstimationColumnName = valueColumnName
  }

  /**
   * This method reduces the overall created datafraame by a set threshold
   *
   * All values which are less good than the threshold will not taken into account
   *
   * @param simDf the resulting dataframe we want to reduce
   * @param threshold the threshold which is taken for reduction as upper bound for distance and lower bound for similarity
   * @return
   */
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

  /**
   * Limits the number of presented nearest neighbors and orders the results
   * @param simDf the similarity evaluated data frame
   * @param k the number of nearest neighbors we search for
   * @param keepKeyUriColumn if we want to keep the column of the fixed uri. so decision between a resulting dataframe of two or three columns
   * @return return dataframe of k nearest neighbor uris in one column, in another column the estimated similarity and in a third column the uri we compared against (third column is optional)
   */
  protected def reduceNnDf(simDf: DataFrame, k: Int, keepKeyUriColumn: Boolean): DataFrame = {
    val tmpDf = if (keepKeyUriColumn) simDf.select("uriA", "uriB", _similarityEstimationColumnName) else simDf.select("uriA", _similarityEstimationColumnName)
    val orderedDf = if (estimatorMeasureType == "distance") tmpDf.orderBy(col(_similarityEstimationColumnName).asc) else tmpDf.orderBy(col(_similarityEstimationColumnName).desc)
    val clippedDf: DataFrame = orderedDf.limit(k)
    clippedDf
  }

  /**
   * This method creates a dataframe which propses for each pair of URI the assigned similarity/distance
   * @param dfA one dataframe formatted with two columns one for the URI as String and one for feature vector as indexed feature vector representation (like from Spark MLlib Count Vectorizer)
   * @param dfB second dataframe to compare entries from dataframe dfA. formetting needed as in dfA
   * @param threshold threshold for minimal distance or similarity final dataframe is filtered for
   * @param valueColumn column name of the resulting similarity/distance column name
   * @return dataframe with the columns for uris and the assigned similarity column
   */
  def similarityJoin(dfA: DataFrame, dfB: DataFrame, threshold: Double = -1.0, valueColumn: String): DataFrame = {

    setSimilarityEstimationColumnName(valueColumn)

    val crossJoinDf = createCrossJoinDF(dfA: DataFrame, dfB: DataFrame)

    val joinDf: DataFrame = crossJoinDf.withColumn(
      valueColumn,
      similarityEstimation(col("featuresA"), col("featuresB"))
    )
    reduceJoinDf(joinDf, threshold)
  }

  /**
   *
   * @param dfA one dataframe formatted with two columns one for the URI as String and one for feature vector as indexed feature vector representation (like from Spark MLlib Count Vectorizer)
   * @param key key vector representation like output from Count Vectorizer from MLlib
   * @param k number of nearest neighbors we want to return
   * @param keyUri name of the uri which is later listed in the resulting dataframe
   * @param valueColumn name of the column of resulting similarities and distances
   * @param keepKeyUriColumn boolean value to decide if the key uri should be presented in a column or not
   * @return resulting dataframe of similar uris with distances/similarities to the given key
   */
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
