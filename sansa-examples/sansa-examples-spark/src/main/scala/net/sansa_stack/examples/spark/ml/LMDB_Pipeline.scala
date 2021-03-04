package net.sansa_stack.examples.spark.ml

import net.sansa_stack.ml.spark.featureExtraction.{FeatureExtractingSparqlGenerator, SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.{NTripleReader, RDFDataFrameReader, RDFReader}
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.riot.Lang
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.{ClusteringEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.sql.{DataFrame, SparkSession}
import net.sansa_stack.query.spark.SPARQLEngine
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

object LMDB_Pipeline {
  def main(args: Array[String]): Unit = {
    var currentTime: Long = System.nanoTime
    println("\nSETUP SPARK SESSION")
    // setup spark session
    val spark = SparkSession.builder
      .appName(s"SampleFeatureExtractionPipeline")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // we need Kryo serialization enabled with some custom serializers
      .config("spark.kryo.registrator", String.join(
        ", ",
        "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
        "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
      // .config("spark.sql.crossJoin.enabled", true) // needs to be enabled if your SPARQL query does make use of cartesian product Note: in Spark 3.x it's enabled by default
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    JenaSystem.init()
    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

    /*
    READ IN DATA
     */
    // spark.rdf(Lang.NTRIPLES)(args(0)).toDS().foreach(println(_))

    println("\nREAD IN DATA")
    val inputFilePath = args(0)
    // val df: DataFrame = spark.read.rdf(Lang.NTRIPLES)(inputFilePath).cache()
    // val dataset = spark.rdf(Lang.NTRIPLES)(inputFilePath).toDS().cache()
    val dataset = NTripleReader.load(
      spark,
      inputFilePath,
      stopOnBadTerm = ErrorParseMode.SKIP,
      stopOnWarnings = WarningParseMode.IGNORE
    ).toDS().cache()
    println(f"\ndata consists of ${dataset.count()} triples")
    dataset.take(n = 10).foreach(println(_))

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

    /*
    CREATE FEATURE EXTRACTING SPARQL
    from a knowledge graph we can either manually create a sparql query or
    we use the auto rdf2feature
     */

    println("\nCREATE FEATURE EXTRACTING SPARQL")

    // OPTION 1
    val (autoSparqlString: String, var_names: List[String]) = FeatureExtractingSparqlGenerator.createSparql(
      dataset,
      "?movie",
      "?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .",
      0,
      1,
      5,
      featuresInOptionalBlocks = true,
    )

    println(autoSparqlString)
    println(autoSparqlString.replace("\n", " "))


    // OPTION 2
    val minimalSparql = """
                          | SELECT
                          | ?movie
                          |
                          |WHERE {
                          |	?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
                          |}
      """.stripMargin

    val oneFeatureSparql = """
                          | SELECT
                          | ?movie ?movie__down_title
                          |
                          |WHERE {
                          |	?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
                          |
                          | OPTIONAL {
                          |		?movie <http://purl.org/dc/terms/title> ?movie__down_title .
                          |	}
                          |}
      """.stripMargin

    val manualSparqlString =
      """
        |SELECT
        |?movie
        |?movie__down_genre__down_film_genre_name
        |?movie__down_date ?movie__down_title
        |?movie__down_runtime ?movie__down_actor__down_actor_name
        |WHERE {
        | ?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
        | OPTIONAL { ?movie <http://purl.org/dc/terms/date> ?movie__down_date . }
        | OPTIONAL { ?movie <http://purl.org/dc/terms/title> ?movie__down_title . }
        | OPTIONAL { ?movie <http://data.linkedmdb.org/movie/runtime> ?movie__down_runtime . }
        | OPTIONAL { ?movie <http://data.linkedmdb.org/movie/actor> ?movie__down_actor . ?movie__down_actor  <http://data.linkedmdb.org/movie/actor_name> ?movie__down_actor__down_actor_name . }
        | OPTIONAL { ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre . ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?movie__down_genre__down_film_genre_name . }
        | }""".stripMargin

    // select the query you want to use or adjust the automatic created one

    val queryString = args(1) match {
      case "0" => minimalSparql
      case "1" => oneFeatureSparql
      case "2" => manualSparqlString
      case "3" => autoSparqlString
      case _ => args(1)
    } // autoSparqlString // manualSparqlString

    println()
    println(queryString)
    println(queryString.replace("\n", " "))

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime


    /*
    FEATURE EXTRACTION OVER SPARQL
    Gain Features from Query
    this creates a dataframe with columns corresponding to Sparql features
     */
    println("\nFEATURE EXTRACTION OVER SPARQL")
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(queryString)
      .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)
    val res = sparqlFrame.transform(dataset)
    res.show(false)

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

    /*
    Create Numeric Feature Vectors
    */
    println("\nSMART VECTOR ASSEMBLER")
    val labelColumnName: String = res.columns(1)
    println(f"column name: $labelColumnName")
    val smartVectorAssembler = new SmartVectorAssembler()
      .setEntityColumn("movie")
      .setLabelColumn(labelColumnName)
    val assembledDf = smartVectorAssembler.transform(res).cache()
    assembledDf.show(false)
    println(f"assembled df has ${assembledDf.count()} rows")

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime


    /*
    APPLY Common SPARK MLlib Example Algorithm
     */
    println("\nAPPLY Common SPARK MLlib Example Algorithm")

    /*
    Indoex Labels
     */
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(assembledDf).setHandleInvalid("skip")
    val assembledDflabeledIndex = labelIndexer.transform(assembledDf)
    assembledDflabeledIndex.show(false)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
      .setNumTrees(10)
    val model = rf.fit(assembledDflabeledIndex.distinct())

    // Make predictions
    val predictions = model.transform(assembledDflabeledIndex)
    // predictions.show(false)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labelsArray(0))
    labelConverter.transform(predictions).select("id", "label", "predictedLabel").show(false)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${(1.0 - accuracy)}")

    println(f"\ntime needed: ${(System.nanoTime - currentTime) / 1e9d}")
    currentTime = System.nanoTime

    // val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    // println(s"Learned classification forest model:\n ${rfModel.toDebugString}")

    /*
    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

     */
  }
}
