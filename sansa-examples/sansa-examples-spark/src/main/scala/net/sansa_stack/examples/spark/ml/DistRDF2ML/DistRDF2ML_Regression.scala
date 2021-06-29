package DistRDF2ML_Evaluation

import java.io.{File, PrintWriter}
import java.util.Calendar

import net.sansa_stack.examples.spark.ml.DistRDF2ML.DistRDF2ML_Evaluation
import net.sansa_stack.ml.spark.featureExtraction.{SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.ml.spark.utils.ML2Graph
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph
import org.apache.jena.graph.Triple
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, collect_list, explode}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.io.Source

object DistRDF2ML_Regression {
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
    val dataset: Dataset[graph.Triple] = {
      NTripleReader.load(
        spark,
        inputPath,
        stopOnBadTerm = ErrorParseMode.SKIP,
        stopOnWarnings = WarningParseMode.IGNORE
      )
        .toDS()
        .cache()
    }
    // dataset.rdd.coalesce(1).saveAsNTriplesFile(args(0).replace(".", " ") + "clean.nt")
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
      (<http://www.w3.org/2001/XMLSchema#int>(?movie__down_runtime) as ?movie__down_runtime_asInt)
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
      .withColumn("movie__down_runtime(Single_NonCategorical_Int)", col("movie__down_runtime(ListOf_NonCategorical_Int)").getItem(0))
      .drop("movie__down_runtime(ListOf_NonCategorical_Int)")
    postprocessedFeaturesDf.show(10, false)

    // postprocessedFeaturesDf.withColumn("tmp", col("movie__down_runtime(ListOf_NonCategorical_Int)").getItem(0)).show(false)

    println("\nSMART VECTOR ASSEMBLER")
    val smartVectorAssembler = new SmartVectorAssembler()
      .setEntityColumn("movie")
      .setLabelColumn("movie__down_runtime(Single_NonCategorical_Int)")
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
    /**
     * drop rows where label is null
     */
    val mlDf = assembledDf
      .filter(col("label").isNotNull)
    mlDf.show(10, false)

    // From here on process is used based on SApache SPark MLlib samples: https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-regression

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = mlDf.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // Train model. This also runs the indexer.
    val model = rf.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("entityID", "prediction", "label", "features").show(10)
    predictions.show()

    val ml2Graph = new ML2Graph()
      .setEntityColumn("entityID")
      .setValueColumn("prediction")

    val metagraph: RDD[Triple] = ml2Graph.transform(predictions)
    metagraph.take(10).foreach(println(_))

    metagraph
      .coalesce(1)
      .saveAsNTriplesFile(args(0) + "someFolder")
  }
}
