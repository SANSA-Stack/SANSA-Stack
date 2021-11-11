package net.sansa_stack.examples.spark.ml.Similarity

import java.util.{Calendar, Date}

import net.sansa_stack.ml.spark.featureExtraction._
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.{DaSimEstimator, JaccardModel}
import net.sansa_stack.ml.spark.utils.FeatureExtractorModel
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.{NTripleReader, RDFReader}
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

    val inputPath = args(0)

    val limitSeeds: Int = args(1).toInt

    var currentTime: Long = System.nanoTime

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

    val timeSparkSetup = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed timeSparkSetup: ${timeSparkSetup}")
    currentTime = System.nanoTime

    // val originalDataRDD = spark.rdf(Lang.TURTLE)("/Users/carstendraschner/Datasets/lmdb.nt").persist()

    val dataset: Dataset[Triple] = NTripleReader
      .load(
        spark,
        inputPath,
        stopOnBadTerm = ErrorParseMode.SKIP,
        stopOnWarnings = WarningParseMode.IGNORE
      )
      .toDS()
      .persist()

    val timeRead = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed timeRead: ${timeRead}")
    currentTime = System.nanoTime


    val dse = new DaSimEstimator()
      // .setSparqlFilter("SELECT ?o WHERE { ?s <https://sansa.sample-stack.net/genre> ?o }")
      .setObjectFilter("http://data.linkedmdb.org/movie/film")
      .setDistSimFeatureExtractionMethod("on")
      .setDistSimThreshold(0.9)
      .setSimilarityCalculationExecutionOrder(Array("writer", "actor", "country", "genre", "runtime", "title")) // Array("runtime")) //
      .setSimilarityValueStreching(false)
      .setImportance(Map("initial_release_date_sim" -> 0.2, "rdf-schema#label_sim" -> 0.0, "runtime_sim" -> 0.2, "writer_sim" -> 0.1, "22-rdf-syntax-ns#type_sim" -> 0.0, "actor_sim" -> 0.3, "genre_sim" -> 0.2))
      .setLimitSeeds(limitSeeds)

    val result: DataFrame = dse
      .transform(dataset)
      .cache()
    println("done with processing")

    result
      .select("uriA", "uriB", "overall_similarity_score")
      .show(false)

    /* result
      .sort(desc("overall_similarity_score"))
      .show(true) */

    println(s"the result has ${result.count()} elements")

    val timeSe4kg = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed Se4kgRead: ${timeSe4kg}")

    spark.stop()
  }
}
