package net.sansa_stack.examples.spark.ml.DistRDF2ML

import net.sansa_stack.ml.spark.featureExtraction.{SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.ml.spark.utils.ML2Graph
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph.Triple
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.{ClusteringEvaluator, RegressionEvaluator}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object DistRDF2ML_Clustering {
  def main(args: Array[String]): Unit = {

    // readIn
    val inputPath: String = args(0) // http://www.cs.toronto.edu/~oktie/linkedmdb/linkedmdb-18-05-2009-dump.nt

    println("\nSETUP SPARK SESSION")
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

    println("\nREAD IN DATA")
    /**
     * Read in dataset of Jena Triple representing the Knowledge Graph
     */
    val dataset = {
      NTripleReader.load(
        spark,
        inputPath,
        stopOnBadTerm = ErrorParseMode.SKIP,
        stopOnWarnings = WarningParseMode.IGNORE
      )
        .toDS()
        .cache()
    }
    println(f"\ndata consists of ${dataset.count()} triples")
    dataset.take(n = 10).foreach(println(_))

    println("\nFEATURE EXTRACTION OVER SPARQL")
    /**
     * The sparql query used to gather features
     */
    val sparqlString = """
      SELECT
      ?movie
      ?movie__down_genre__down_film_genre_name
      ?movie__down_title
      ?movie__down_runtime
      ?movie__down_actor__down_actor_name

      WHERE {
      ?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
      ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre . ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?movie__down_desiredGenre__down_film_genre_name .

      OPTIONAL { ?movie <http://purl.org/dc/terms/title> ?movie__down_title . }
      OPTIONAL { ?movie <http://data.linkedmdb.org/movie/runtime> ?movie__down_runtime . }
      OPTIONAL { ?movie <http://data.linkedmdb.org/movie/actor> ?movie__down_actor . ?movie__down_actor <http://data.linkedmdb.org/movie/actor_name> ?movie__down_actor__down_actor_name . }
      OPTIONAL { ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre . ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?movie__down_genre__down_film_genre_name . }

      FILTER (?movie__down_desiredGenre__down_film_genre_name = 'Superhero' || ?movie__down_desiredGenre__down_film_genre_name = 'Fantasy' )
      }"""

    /**
     * transformer that collect the features from the Dataset[Triple] to a common spark Dataframe
     * collapsed
     * by column movie
     */
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(sparqlString)
      .setCollapsByKey(true)
      .setCollapsColumnName("movie")

    /**
     * dataframe with resulting features
     * in this collapsed by the movie column
     */
    val extractedFeaturesDf = sparqlFrame
      .transform(dataset)
      .cache()

    /**
     * feature descriptions of the resulting collapsed dataframe
     */
    val featureDescriptions = sparqlFrame.getFeatureDescriptions()
    println(s"Feature decriptions are:\n${featureDescriptions.mkString(",\n")}")

    extractedFeaturesDf.show(10, false)
    // extractedFeaturesDf.schema.foreach(println(_))

    println("FEATURE EXTRACTION POSTPROCESSING")
    /**
     * Here we adjust things in dataframe which do not fit to our expectations like:
     * the fact that the runtime is in rdf data not annotated to be a double
     * but easy castable
     */
    val postprocessedFeaturesDf = extractedFeaturesDf
      .withColumn("movie__down_runtime(ListOf_NonCategorical_Int)", col("movie__down_runtime(ListOf_NonCategorical_String)").cast("array<int>"))
      .drop("movie__down_runtime(ListOf_NonCategorical_String)")
    postprocessedFeaturesDf.show(10, false)

    // postprocessedFeaturesDf.withColumn("tmp", col("movie__down_runtime(ListOf_NonCategorical_Int)").getItem(0)).show(false)

    println("\nSMART VECTOR ASSEMBLER")
    val smartVectorAssembler = new SmartVectorAssembler()
      .setEntityColumn("movie")
      // .setLabelColumn("movie__down_runtime(Single_NonCategorical_Int)")
      .setNullReplacement("string", "")
      .setNullReplacement("digit", -1)
      .setWord2VecSize(5)
      .setWord2VecMinCount(1)
      // .setWord2vecTrainingDfSizeRatio(svaWord2vecTrainingDfSizeRatio)

    val assembledDf: DataFrame = smartVectorAssembler
     .transform(postprocessedFeaturesDf)
     .cache()

    assembledDf.show(10, false)


    println("\nAPPLY Common SPARK MLlib Example Algorithm")
    val kmeans = new KMeans()
      .setK(5)
      .setSeed(1L)
    val model = kmeans.fit(assembledDf)

    // Make predictions
    val predictions = model.transform(assembledDf)
    predictions
      .select("entityID", "prediction")
      .show(false)

    val ml2Graph = new ML2Graph()
      .setEntityColumn("entityID")
      .setValueColumn("prediction")

    val metagraph: RDD[Triple] = ml2Graph.transform(predictions)
    metagraph.take(10).foreach(println(_))

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
  }
}
