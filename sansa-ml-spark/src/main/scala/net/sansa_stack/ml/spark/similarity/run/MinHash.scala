package net.sansa_stack.ml.spark.similarity.run

import net.sansa_stack.ml.spark.utils.RDF_Feature_Extractor
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

object MinHash {
  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {

    // parameters
    // spark session parameter

    // rdf readin parameters
    val input = "/Users/carstendraschner/GitHub/SANSA-ML/sansa-ml-spark/src/main/resources/movie.nt"
    val lang = Lang.NTRIPLES
    // feature extraction parameter
    val mode = "at"
    val feature_extractor_uri_column_name = "uri"
    val feature_extractor_features_column_name = "fe_features"
    // countvectorizer parameters
    val count_vectorizer_features_column_name = "cv_features"
    val cv_vocab_size = 1000000
    val cv_min_document_frequency = 1


    // start spark session
    val spark = SparkSession.builder
      .appName(s"MinHash  tryout") // TODO where is this displayed?
      .master("local[*]") // TODO why do we need to specify this?
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO what is this for?
      .getOrCreate()
    import spark.implicits._ // TODO does anyone know for which purposes we need

    // read in data of sansa rdf
    val triples = spark.rdf(lang)(input)

    // create dataframe from rdf rdd by rdffeature extractor in sansa ml layer
    val feature_extractor = new RDF_Feature_Extractor()
    feature_extractor.set_mode(mode)
    feature_extractor.set_uri_key_column_name(feature_extractor_uri_column_name)
    feature_extractor.set_features_column_name(feature_extractor_features_column_name)
    val fe_features: DataFrame = feature_extractor.transform(triples)

    // Count Vectorizer from MLlib
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol(feature_extractor_features_column_name)
      .setOutputCol(count_vectorizer_features_column_name)
      .setVocabSize(cv_vocab_size)
      .setMinDF(cv_min_document_frequency) // TODO maybe later higher
      .fit(fe_features)
    // val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType) this line is not needed because all uris have features by feature extraction algo
    val cv_features = cvModel.transform(fe_features).select(col(feature_extractor_uri_column_name), col(count_vectorizer_features_column_name)) // .filter(isNoneZeroVector(col(count_vectorizer_features_column_name)))


    spark.stop()
  }
}
