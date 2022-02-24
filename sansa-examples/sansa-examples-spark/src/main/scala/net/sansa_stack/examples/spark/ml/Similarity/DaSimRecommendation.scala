package net.sansa_stack.examples.spark.ml.Similarity

import net.sansa_stack.ml.spark.similarity.similarityEstimationModels.DaSimEstimator
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph.Triple
import org.apache.jena.sys.JenaSystem
import org.apache.spark.sql._
import org.apache.spark.sql.functions.desc


object DaSimRecommendation {
  def main(args: Array[String]): Unit = {

    val inputPath = args(0)

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



    val dse = new DaSimEstimator()
      .setObjectFilter("http://data.linkedmdb.org/movie/film")
      .setDistSimFeatureExtractionMethod("os")
      .setSimilarityValueStreching(false)
      .setImportance(Map(
        "initial_release_date_sim" -> 0.2,
        "rdf-schema#label_sim" -> 0.0,
        "runtime_sim" -> 0.2,
        "writer_sim" -> 0.1,
        "22-rdf-syntax-ns#type_sim" -> 0.0,
        "actor_sim" -> 0.3, "genre_sim" -> 0.2))
      .setVerbose(false)

    val result: DataFrame = dse
      .transform(dataset)
      .cache()
    println("done with processing")

    val metagraph: Dataset[Triple] = dse
      .semantification(result)
      .toDS()
      .cache()

    /** Merge KGs */
    println("Enriched KG")
    val enrichedKG = dataset.union(metagraph.filter(! _.getPredicate().isLiteral))
    println("original kg size: " + dataset.collect.size + ", the semantified result has: " + metagraph.collect.size + ", and the merged KG has size of: " + enrichedKG.collect.size)

    enrichedKG.foreach(println(_))


    /** Recommendations */
    println("Recommendations")

    val seedMovie = "<https://sansa.sample-stack.net/film/3>"

    val sparql = s"""
      SELECT ?movieURI ?name ?value
      WHERE {
      ?simAs <https://sansa-stack.net/sansaVocab/element> ?seedMovie .
      ?simAs <https://sansa-stack.net/sansaVocab/element> ?movieURI .
      ?simAs <https://sansa-stack.net/sansaVocab/value> ?value .
      ?movieURI <http://www.w3.org/2000/01/rdf-schema#label> ?name .

      FILTER ( ?seedMovie = $seedMovie) .
      FILTER (?movieURI != $seedMovie) .
      }
      """

    import net.sansa_stack.ml.spark.featureExtraction.SparqlFrame
    val sf = new SparqlFrame()
      .setSparqlQuery(sparql)

    println(s"baseline movie: $seedMovie")

    sf
      .transform(enrichedKG).orderBy(desc("value"))
      .show(false)

    spark.stop()
  }
}
