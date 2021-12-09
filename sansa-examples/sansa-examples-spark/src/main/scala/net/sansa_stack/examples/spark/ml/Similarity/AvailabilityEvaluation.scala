package net.sansa_stack.examples.spark.ml.Similarity

import net.sansa_stack.ml.spark.featureExtraction.{SmartFeatureExtractor, SparqlFrame}
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.DaSimEstimator
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object AvailabilityEvaluation {
  def main(args: Array[String]): Unit = {

    val path = args(0)

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
        path,
        stopOnBadTerm = ErrorParseMode.SKIP,
        stopOnWarnings = WarningParseMode.IGNORE
      )
      .toDS()
      .cache()

    println(dataset.count())

    val timeRead = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed timeRead: ${timeRead}")
    currentTime = System.nanoTime

    val sfeSparqlFilter = new SmartFeatureExtractor()
      // .setEntityColumnName("s")
      .setObjectFilter("http://data.linkedmdb.org/movie/film")

    val feDf2 = sfeSparqlFilter
      .transform(dataset)
    feDf2
      .show()

    val se4kg = new DaSimEstimator()

    se4kg.calculateAvailability(feDf2).foreach(println(_))



    spark.stop()

  }
}
