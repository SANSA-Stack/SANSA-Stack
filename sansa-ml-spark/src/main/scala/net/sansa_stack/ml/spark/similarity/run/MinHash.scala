package net.sansa_stack.ml.spark.similarity.run

import java.util.Calendar

import net.sansa_stack.ml.spark.utils.{RDF_Feature_Extractor, Similarity_Experiment_Meta_Graph_Factory}
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, MinHashLSH}
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
    // minhash parameters
    val minhash_number_hash_tables = 1
    val minhash_hash_column_name = "hashValues"
    val minhash_threshold_max_distance = 0.8
    val minhash_distance_column_name = "distance"

    // metagraph creator
    // Strings for relation names, maybe this can be later defined in an onthology and only be imported here
    val metagraph_element_relation = "element"
    val metagraph_value_relation = "value"
    val metagraph_experiment_type_relation = "experiment_type"
    val metagraph_experiment_name_relation = "experiment_name"
    val metagraph_experiment_measurement_type_relation = "experiment_measurement_type"
    val metagraph_experiment_datetime_relation = "experiment_datetime"
    // Strings for uris and literals
    val metagraph_experiment_name = "Spark_Min_Hash"
    val metagraph_experiment_type = "Sematic Similarity Estimation"
    val metagraph_experiment_measurement_type = "distance"

    // metagraph store parameters
    val output = "/Users/carstendraschner/Downloads/experiment_results"

    // start spark session
    val spark = SparkSession.builder
      .appName(s"MinHash") // TODO where is this displayed?
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
      .setMinDF(cv_min_document_frequency)
      .fit(fe_features)
    // val isNoneZeroVector = udf({ v: Vector => v.numNonzeros > 0 }, DataTypes.BooleanType) this line is not needed because all uris have features by feature extraction algo
    val cv_features = cvModel.transform(fe_features).select(col(feature_extractor_uri_column_name), col(count_vectorizer_features_column_name)) // .filter(isNoneZeroVector(col(count_vectorizer_features_column_name)))

    // MinHash
    val mh = new MinHashLSH()
      .setNumHashTables(minhash_number_hash_tables)
      .setInputCol(count_vectorizer_features_column_name)
      .setOutputCol(minhash_hash_column_name)
    val model = mh.fit(cv_features)
    // min Hash crosstable for all semantc similarities
    val element_column_name_A = feature_extractor_uri_column_name + "_A"
    val element_column_name_B = feature_extractor_uri_column_name + "_B"
    val all_pair_similarity_df = model.approxSimilarityJoin(cv_features, cv_features, minhash_threshold_max_distance, minhash_distance_column_name)
      .withColumn(element_column_name_A, col("datasetA").getField(feature_extractor_uri_column_name))
      .withColumn(element_column_name_B, col("datasetB").getField(feature_extractor_uri_column_name))
      .select(element_column_name_A, element_column_name_B, minhash_distance_column_name)
    // all_pair_similarity_df.show(false)

    // Metagraph creation
    val similarity_metagraph_creator = new Similarity_Experiment_Meta_Graph_Factory()
    val experiment_metagraph = similarity_metagraph_creator.transform(
      all_pair_similarity_df
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
