package net.sansa_stack.ml.spark.similarity.run

import java.util.Calendar

import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.TverskyModel
import net.sansa_stack.ml.spark.utils.{FeatureExtractorModel, SimilarityExperimentMetaGraphFactory}
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.linalg.Vector

object Tversky {
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

    // Tversky parameter
    val threshold_min_similarity = 0.2 // !!! and never should be hardcoded
    val estimation_column_name = "tverskySimilarity" // !!!
    val alpha: Double = 0.5
    val betha: Double = 0.5

    // metagraph creator
    // Strings for relation names, maybe this can be later defined in an onthology and only be imported here
    val metagraph_element_relation = "element"
    val metagraph_value_relation = "value"
    val metagraph_experiment_type_relation = "experiment_type"
    val metagraph_experiment_name_relation = "experiment_name"
    val metagraph_experiment_measurement_type_relation = "experiment_measurement_type"
    val metagraph_experiment_datetime_relation = "experiment_datetime"
    // Strings for uris and literals
    val metagraph_experiment_name = "Tversky"  // !!!
    val metagraph_experiment_type = "Sematic Similarity Estimation"
    val metagraph_experiment_measurement_type = "distance"

    // metagraph store parameters
    val output = "/Users/carstendraschner/Downloads/experiment_results"

    // start spark session
    val spark = SparkSession.builder
      .appName(s"TverskySimilarityEvaluation") // TODO where is this displayed?  // !!!
      .master("local[*]") // TODO why do we need to specify this?
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO what is this for?
      .getOrCreate() // TODO does anyone know for which purposes we need

    // read in data of sansa rdf
    val triples = spark.rdf(lang)(input)

    triples.take(5).foreach(println(_))

    // create dataframe from rdf rdd by rdffeature extractor in sansa ml layer
    val feature_extractor = new FeatureExtractorModel()
    feature_extractor.setMode(mode)
    // feature_extractor.set_uri_key_column_name(feature_extractor_uri_column_name)
    //f eature_extractor.set_features_column_name(feature_extractor_features_column_name)
    val fe_features: DataFrame = feature_extractor.transform(triples)

    fe_features.show(false)

    // Count Vectorizer from MLlib
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol(feature_extractor_features_column_name)
      .setOutputCol(count_vectorizer_features_column_name)
      .setVocabSize(cv_vocab_size)
      .setMinDF(cv_min_document_frequency)
      .fit(fe_features)
    // val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType) this line is not needed because all uris have features by feature extraction algo
    val cv_features = cvModel.transform(fe_features).select(col(feature_extractor_uri_column_name), col(count_vectorizer_features_column_name)) // .filter(isNoneZeroVector(col(count_vectorizer_features_column_name)))

    cv_features.show(false)

    // Similarity Estimation
    val similarityModel = new TverskyModel()
      .setUriColumnNameDfA(feature_extractor_uri_column_name)
      .setUriColumnNameDfB(feature_extractor_uri_column_name)
      .setFeaturesColumnNameDfA(count_vectorizer_features_column_name)
      .setFeaturesColumnNameDfB(count_vectorizer_features_column_name)
      .setAlpha(alpha)
      .setBeta(betha)
    // model evaluations
    // all pair
    val allPairSimilarityDf = similarityModel.similarityJoin(cv_features, cv_features, threshold_min_similarity)
    allPairSimilarityDf.show(false)
    // nearest neighbor
    similarityModel
      .setUriColumnNameDfA(feature_extractor_uri_column_name)
      .setUriColumnNameDfB(feature_extractor_uri_column_name)
      .setFeaturesColumnNameDfA(count_vectorizer_features_column_name)
      .setFeaturesColumnNameDfB(count_vectorizer_features_column_name)
    val key: Vector = cv_features.select(count_vectorizer_features_column_name).collect()(0)(0).asInstanceOf[Vector]
    val nnSimilarity_df = similarityModel.nearestNeighbors(cv_features, key, 10, "theFirstUri", keepKeyUriColumn = true)
    nnSimilarity_df.show(false)

    // Metagraph creation
    val similarity_metagraph_creator = new SimilarityExperimentMetaGraphFactory()
    val experiment_metagraph = similarity_metagraph_creator.transform(
      allPairSimilarityDf
    )(
      metagraph_experiment_name,
      metagraph_experiment_type,
      metagraph_experiment_measurement_type
    )(
      metagraph_element_relation,
      metagraph_value_relation,
      metagraph_experiment_type_relation,
      metagraph_experiment_name_relation,
      metagraph_experiment_measurement_type_relation,
      metagraph_experiment_datetime_relation)

    experiment_metagraph.take(5).foreach(println(_))

    // Store metagraph over sansa rdf layer
    // dt to enforce different outputstrings so no conflicts occur
    val dt = Calendar.getInstance().getTime()
      .toString // make string out of it, in future would be better to allow date nativly in rdf
      .replaceAll("\\s", "") // remove spaces to reduce confusions with some foreign file readers
      .replaceAll(":", "")
    experiment_metagraph.coalesce(1, shuffle = true).saveAsNTriplesFile(output + dt)

    spark.stop()
  }
}
