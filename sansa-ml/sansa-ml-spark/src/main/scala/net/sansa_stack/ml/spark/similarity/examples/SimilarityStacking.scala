package net.sansa_stack.ml.spark.similarity.examples

import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.{JaccardModel, MinHashModel}
import net.sansa_stack.ml.spark.utils.FeatureExtractorModel
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

object SimilarityStacking {

  def main(args: Array[String]): Unit = {

    // setup spark session
    val spark = SparkSession.builder()
      .appName(s"MinMal Semantic Similarity Estimation Calls")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // cause of jena issue hint by clause to do it that way
    JenaSystem.init()

    // define inputpath if it is not parameter
    val inputPath = "./sansa-ml/sansa-ml-spark/src/main/resources/movieData/movie.nt"

    // read in data as Data`Frame
    val triplesDf: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputPath).cache()

    triplesDf.show(false)

    // feature extraction
    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("an")
    val extractedFeaturesDataFrame = featureExtractorModel
      .transform(triplesDf)
      .filter(t => t.getAs[String]("uri").startsWith("m"))

    // filter for relevant URIs e.g. only movies
    val filteredFeaturesDataFrame = extractedFeaturesDataFrame.filter(t => t.getAs[String]("uri").startsWith("m")).cache()
    filteredFeaturesDataFrame.show(false)

    // count Vectorization
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .fit(filteredFeaturesDataFrame)
    val tmpCvDf: DataFrame = cvModel.transform(filteredFeaturesDataFrame)
    val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 })
    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf.filter(isNoneZeroVector(col("vectorizedFeatures"))).select("uri", "vectorizedFeatures").cache()
    countVectorizedFeaturesDataFrame.show(false)

    // minHash similarity estimation
    val minHashModel: MinHashModel = new MinHashModel()
      .setInputCol("vectorizedFeatures")
    val aps = minHashModel.similarityJoin(countVectorizedFeaturesDataFrame, countVectorizedFeaturesDataFrame, 1.0, "minHashDistance")

    // show resulting similary estimation of APS
    aps.show(false)

    // regain cv vectors over join
    // alternatively the select statement in the resepctive similarity estiamtor can be changed
    val tmp = aps
      .join(countVectorizedFeaturesDataFrame.withColumnRenamed("uri", "uriA"), "uriA")
      .withColumnRenamed("vectorizedFeatures", "datasetA")
      .join(countVectorizedFeaturesDataFrame.withColumnRenamed("uri", "uriB"), "uriB")
      .withColumnRenamed("vectorizedFeatures", "datasetB")
      .drop("distance")
    tmp.show()

    // setup jaccard model
    val jaccardModel: JaccardModel = new JaccardModel()
      .setInputCol("vectorizedFeatures")

    // call alternative similarity estimation like jaccard
    tmp.withColumn("jaccard", jaccardModel.similarityEstimation(col("datasetA"), col("datasetB")))
      .select("uriA", "uriB", "jaccard")
      .show(false)
  }
}
