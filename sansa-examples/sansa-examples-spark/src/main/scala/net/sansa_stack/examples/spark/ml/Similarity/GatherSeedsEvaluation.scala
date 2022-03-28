package net.sansa_stack.examples.spark.ml.Similarity

import net.sansa_stack.ml.spark.featureExtraction.SparqlFrame
import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.DaSimEstimator
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph.Triple
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object GatherSeedsEvaluation {
  def main(args: Array[String]): Unit = {

    val objectFilter = "http://data.linkedmdb.org/movie/film"
    val sparqlFilter = "SELECT ?seed WHERE {?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .}"


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
        "/Users/carstendraschner/Datasets/lmdb.nt",
        stopOnBadTerm = ErrorParseMode.SKIP,
        stopOnWarnings = WarningParseMode.IGNORE
      )
      .toDS()
      .cache()

    println(dataset.count())

    val timeRead = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed timeRead: ${timeRead}")
    currentTime = System.nanoTime

    val seeds: DataFrame = {
      if (objectFilter != null) {
        println("filter by object")
        dataset
          .filter(t => ((t.getObject.toString().equals(objectFilter))))
          .rdd
          .toDF()
          .select("s")
          .withColumnRenamed("s", "seed")
      }
      else if (sparqlFilter != null) {
        println("filter by sparql")
        val sf = new SparqlFrame()
          .setSparqlQuery(sparqlFilter)
        val tmpDf: DataFrame = sf
          .transform(dataset)
        val cn: Array[String] = tmpDf.columns
        tmpDf
          .withColumnRenamed(cn(0), "seed")
      }
      else {
        val tmpSchema = new StructType()
          .add(StructField("seed", StringType, true))

        spark.createDataFrame(
          dataset
            .rdd
            .flatMap(t => Seq(t.getSubject, t.getObject))
            .filter(_.isURI)
            .map(_.toString())
            .distinct
            .map(Row(_)),
          tmpSchema
        )
      }
    }

    println("seeds count: ", seeds.count())

    val timeSe4kg = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed gather seeds: ${timeSe4kg}")

    spark.stop()
  }
}
