package net.sansa_stack.examples.spark.ml.Similarity

import java.util.{Calendar, Date}

import net.sansa_stack.ml.spark.featureExtraction._
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.{DaSimEstimator, JaccardModel}
import net.sansa_stack.ml.spark.utils.FeatureExtractorModel
import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, HashingTF, IDF}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._


object DaSimEvaluation {
  def main(args: Array[String]): Unit = {
    val spark = {
      SparkSession.builder
        .appName(s"SampleFeatureExtractionPipeline")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", String.join(", ",
          "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
          "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
        .getOrCreate()
    }
    spark.sparkContext.setLogLevel("ERROR")
    JenaSystem.init()
    val lang = Lang.TURTLE
    val originalDataRDD = spark.rdf(lang)("/Users/carstendraschner/Datasets/sampleMovieDB.nt").persist()

    val dataset: Dataset[Triple] = originalDataRDD
      .toDS()
      .cache()

    val dse = new DaSimEstimator()
      // .setSparqlFilter("SELECT ?o WHERE { ?s <https://sansa.sample-stack.net/genre> ?o }")
      .setObjectFilter("http://data.linkedmdb.org/movie/film")
    dse.transform(dataset)
      .show(false)
  }
}
