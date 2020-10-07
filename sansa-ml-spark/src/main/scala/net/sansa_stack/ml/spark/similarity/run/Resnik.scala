package net.sansa_stack.ml.spark.similarity.run

import net.sansa_stack.ml.spark.utils.FeatureExtractorModel
import net.sansa_stack.owl.spark.dataset
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Resnik {
  def main(args: Array[String]): Unit = {

    // start spark session
    val spark = SparkSession.builder
      .appName(s"JaccardSimilarityEvaluation")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // define inputpath if it is not parameter
    val inputPath = "/Users/carstendraschner/GitHub/SANSA-ML/sansa-ml-spark/src/main/resources/movie.nt"

    // read in data as Data`Frame
    val triplesDf: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputPath)

    triplesDf.show()

    // feature extraction
    val featureExtractorModel = new FeatureExtractorModel()
      .setMode("an")
    val extractedFeaturesDataFrame = featureExtractorModel.transform(triplesDf)
    extractedFeaturesDataFrame.show()

    // count Vectorization
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("extractedFeatures")
      .setOutputCol("vectorizedFeatures")
      .fit(extractedFeaturesDataFrame)
    val tmpCvDf: DataFrame = cvModel.transform(extractedFeaturesDataFrame)
    val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType)
    val countVectorizedFeaturesDataFrame: DataFrame = tmpCvDf.filter(isNoneZeroVector(col("vectorizedFeatures"))).select("uri", "vectorizedFeatures")
    countVectorizedFeaturesDataFrame.show()

    // similarity estimations
    // for nearestNeighbors we need one key which is a Vector to search for NN
    val sample_key: Vector = countVectorizedFeaturesDataFrame.take(1)(0).getAs[Vector]("vectorizedFeatures")

    val idf = new IDF().setInputCol("vectorizedFeatures").setOutputCol("features")
    val idfModel = idf.fit(countVectorizedFeaturesDataFrame)

    val rescaledData = idfModel.transform(countVectorizedFeaturesDataFrame)
    rescaledData.show(false)

    spark.stop()

  }
}
/*
class InformationContentModel {
  def calcIC(df: DataFrame): DataFrame = {

    val ds: Dataset[(String, String, String)] = dataset.as[(String, String, String)]
    // collect all element occurences
    val drdd = ds.rdd

    val occurences = drdd.map(_._3).filter(!_.contains("\""))

    val occurenceMap = drdd
      .flatMap(t => Seq((t._1, 1), (t._3, 1)))

    val numberOccurences = occurenceMap.reduceByKey(_ + _)

  }
}*/
