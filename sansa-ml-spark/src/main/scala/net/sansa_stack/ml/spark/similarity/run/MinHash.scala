package net.sansa_stack.ml.spark.similarity.run

import net.sansa_stack.ml.spark.utils.RDF_Feature_Extractor
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object MinHash {
  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {
    // start spark session
    val spark = SparkSession.builder
      .appName(s"MinHash  tryout") // TODO where is this displayed?
      .master("local[*]") // TODO why do we need to specify this?
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // TODO what is this for?
      .getOrCreate()
    import spark.implicits._ // TODO does anyone know for which purposes we need

    // read in data of sansa rdf
    val input = "/Users/carstendraschner/GitHub/SANSA-ML/sansa-ml-spark/src/main/resources/movie.nt"
    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)

    // create dataframe from rdf rdd by rdffeature extractor in sansa ml layer
    val feature_extractor = new RDF_Feature_Extractor()
    feature_extractor.set_mode("at")
    feature_extractor.set_uri_key_column_name("uri")
    feature_extractor.set_features_column_name("features")
    val rdf_features: DataFrame = feature_extractor.transform(triples)
    rdf_features.show(false)

    spark.stop()
  }
}
