package net.sansa_stack.ml.spark.similarity.run

import java.util.Calendar

import net.sansa_stack.ml.spark.utils.{FeatureExtractorModel, SimilarityExperimentMetaGraphFactory}
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, MinHashLSH, MinHashLSHModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit, udf}
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
    val input: String = "/Users/carstendraschner/GitHub/SANSA-ML/sansa-ml-spark/src/main/resources/movie.nt"
    val lang: Lang = Lang.NTRIPLES

    // feature extraction parameter
    val mode: String = "at"
    val feature_extractor_uri_column_name: String = "uri"
    val feature_extractor_features_column_name: String = "fe_features"

    // countvectorizer parameters
    val count_vectorizer_features_column_name: String = "cv_features"
    val cv_vocab_size: Int = 1000000
    val cv_min_document_frequency: Int = 1

    // minhash parameters
    val minhash_number_hash_tables: Int = 5
    val minhash_hash_column_name: String = "hashValues"
    val minhash_threshold_max_distance: Double = 0.8
    val minhash_distance_column_name: String = "distance"
    val minhash_nn_k: Int = 20

    // metagraph creator
    // Strings for relation names, maybe this can be later defined in an onthology and only be imported here
    val metagraph_element_relation: String = "element"
    val metagraph_value_relation: String = "value"
    val metagraph_experiment_type_relation: String = "experiment_type"
    val metagraph_experiment_name_relation: String = "experiment_name"
    val metagraph_experiment_measurement_type_relation: String = "experiment_measurement_type"
    val metagraph_experiment_datetime_relation: String = "experiment_datetime"
    // Strings for uris and literals
    val metagraph_experiment_name: String = "Spark_Min_Hash"
    val metagraph_experiment_type: String = "Sematic Similarity Estimation"
    val metagraph_experiment_measurement_type: String = "distance"

    // metagraph store parameters
    val output: String = "/Users/carstendraschner/Downloads/experiment_results"

    // start spark session
    val spark = SparkSession.builder
      .appName(s"MinHash") // TODO where is this displayed?
      .master("local[*]") // TODO why do we need to specify this?
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO what is this for?
      .getOrCreate()
    import spark.implicits._ // TODO does anyone know for which purposes we need

    // read in data of sansa rdf
    val triples: DataFrame = spark.read.rdf(lang)(input)

    // triples.take(5).foreach(println(_))

    // create dataframe from rdf rdd by rdffeature extractor in sansa ml layer
    val feature_extractor: FeatureExtractorModel = new FeatureExtractorModel()
      .setMode(mode)
      // .set_uri_key_column_name(feature_extractor_uri_column_name)
      // .set_features_column_name(feature_extractor_features_column_name)
    val fe_features: DataFrame = feature_extractor.transform(triples)

    fe_features.show(false)

    // Count Vectorizer from MLlib
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol(feature_extractor_features_column_name)
      .setOutputCol(count_vectorizer_features_column_name)
      .setVocabSize(cv_vocab_size)
      .setMinDF(cv_min_document_frequency)
      .fit(fe_features)
    val cv_features: DataFrame = cvModel.transform(fe_features).select(col(feature_extractor_uri_column_name), col(count_vectorizer_features_column_name)) // .filter(isNoneZeroVector(col(count_vectorizer_features_column_name)))

    cv_features.show(false)

    // MinHash
    val mh: MinHashLSH = new MinHashLSH()
      .setNumHashTables(minhash_number_hash_tables)
      .setInputCol(count_vectorizer_features_column_name)
      .setOutputCol(minhash_hash_column_name)
    val model: MinHashLSHModel = mh.fit(cv_features)

    model.transform(cv_features).show(false)
    // min Hash crosstable for all semantc similarities
    val element_column_name_A = feature_extractor_uri_column_name + "_A"
    val element_column_name_B = feature_extractor_uri_column_name + "_B"
    val all_pair_similarity_df: DataFrame = model.approxSimilarityJoin(cv_features, cv_features, minhash_threshold_max_distance, minhash_distance_column_name)
      .withColumn(element_column_name_A, col("datasetA").getField(feature_extractor_uri_column_name))
      .withColumn(element_column_name_B, col("datasetB").getField(feature_extractor_uri_column_name))
      .select(element_column_name_A, element_column_name_B, minhash_distance_column_name)
    all_pair_similarity_df.show(false)
    // all_pair_similarity_df.show(false)
    val key: Vector = cv_features.select(count_vectorizer_features_column_name).collect()(0)(0).asInstanceOf[Vector]
    val nn_df: DataFrame = model
      .approxNearestNeighbors(cv_features, key, minhash_nn_k, minhash_distance_column_name)
      .withColumn("key_column", lit("key_uri")).select("key_column", feature_extractor_uri_column_name, minhash_distance_column_name)
    nn_df.show(false)

    // Metagraph creation
    val result_df_to_store = nn_df // all_pair_similarity_df
    val similarity_metagraph_creator = new SimilarityExperimentMetaGraphFactory()
    val experiment_metagraph = similarity_metagraph_creator.transform(
      result_df_to_store
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
