package net.sansa_stack.examples.spark.ml.DistRDF2ML

import net.sansa_stack.ml.spark.featureExtraction.{SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.ml.spark.utils.ML2Graph
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph.Triple
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DistRDF2ML_Classification {
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
      (<http://www.w3.org/2001/XMLSchema#int>(?movie__down_runtime) as ?movie__down_runtime_asInt)
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

    println("\nSMART VECTOR ASSEMBLER")
    val smartVectorAssembler = new SmartVectorAssembler()
      .setEntityColumn("movie")
      .setLabelColumn("movie__down_genre__down_film_genre_name(ListOf_Categorical_String)")
      .setNullReplacement("string", "")
      .setNullReplacement("digit", -1)
      .setWord2VecSize(5)
      .setWord2VecMinCount(1)
      // .setWord2vecTrainingDfSizeRatio(svaWord2vecTrainingDfSizeRatio)

    val assembledDf: DataFrame = smartVectorAssembler
     .transform(extractedFeaturesDf)
     .cache()

    assembledDf.show(10, false)

    println("\nAPPLY Common SPARK MLlib Example Algorithm")
    /**
     * drop rows where label is null
     */
    val mlDf = assembledDf
      .filter(col("label").isNotNull)
      .withColumn("label", col("label").getItem(0))
      // .select(col("entityID"), explode(col("label")), col("features"))
    mlDf.show(10, false)

    // From here on process is used based on Apache SPark MLlib samples: https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-regression

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(mlDf)
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(mlDf)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = mlDf.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(20)

    val ml2Graph = new ML2Graph()
      .setEntityColumn("entityID")
      .setValueColumn("predictedLabel")

    val metagraph: RDD[Triple] = ml2Graph.transform(predictions)
    metagraph.take(10).foreach(println(_))

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    // val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    // println("Learned classification forest model:\n" + rfModel.toDebugString)
  }
}
