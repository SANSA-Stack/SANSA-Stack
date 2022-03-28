package net.sansa_stack.examples.spark.ml.DistRDF2ML

import java.io.{File, PrintWriter}
import java.util.Calendar

import net.sansa_stack.ml.spark.featureExtraction.{SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.{NTripleReader, RDFReader}
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.riot.Lang
import org.apache.jena.graph.Triple
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.io.Source

object DistRDF2ML_DataSizeEvaluation {
  def main(args: Array[String]): Unit = {

    // readIn
    val inputPath: String = args(0)

    // sparqlFrame
    val sparqlString: String = args(1)
    val sparqlFrameCollapse: Boolean = true

    // smartVector assembler
    val svaEntityColumn: String = sparqlString.split("\\?")(1).stripSuffix(" ").stripPrefix(" ")
    val svaLabelColumn: String = sparqlString.split("\\?")(2).stripSuffix(" ").stripPrefix(" ")
    val svaWord2VecSize: Int = 2
    val svaWord2VecMinCount: Int = 1
    val svaWord2vecTrainingDfSizeRatio: Double = 1

    // datetime
    val datetime: String = Calendar.getInstance().getTime().toString

    // further comments
    val comments: String = args(3)

    // write
    val writeFolderPath: String = args(2)

    println("\nSETUP SPARK SESSION")
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
    println(f"\ntime needed: ${timeSparkSetup}")
    println("spark information")
    println(spark.sparkContext.getExecutorMemoryStatus)
    spark.sparkContext.getConf.getAll.foreach(println)

    println("\nREAD IN DATA")
    currentTime = System.nanoTime
    val dataset: Dataset[Triple] = spark
      .rdf(Lang.TTL)(inputPath)
      .toDS()
      .persist()

    println(f"\ndata consists of ${dataset.count()} triples")
    dataset.take(n = 10).foreach(println(_))
    val timeReadIn = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed: ${timeReadIn}")


    println("\nFEATURE EXTRACTION OVER SPARQL")
    /*
        val sparqlstring1 = """
          SELECT ?movie ?movie__down_genre__down_film_genre_name ?movie__down_date ?movie__down_title ?movie__down_filmid ?movie__down_runtime ?movie__down_actor__down_actor_name ?movie__down_genre__down_film_genre_name ?movie__down_country__down_country_areaInSqKm
          WHERE {
          ?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
          OPTIONAL { ?movie <http://purl.org/dc/terms/date> ?movie__down_date . }
          OPTIONAL { ?movie <http://purl.org/dc/terms/title> ?movie__down_title . }
          OPTIONAL { ?movie <http://data.linkedmdb.org/movie/filmid> ?movie__down_filmid . }
          OPTIONAL { ?movie <http://data.linkedmdb.org/movie/runtime> ?movie__down_runtime . }
          OPTIONAL { ?movie <http://data.linkedmdb.org/movie/actor> ?movie__down_actor . ?movie__down_actor <http://data.linkedmdb.org/movie/actor_name> ?movie__down_actor__down_actor_name . }
          OPTIONAL { ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre . ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?movie__down_genre__down_film_genre_name . }
          OPTIONAL { ?movie__up_country <http://data.linkedmdb.org/movie/country> ?movie . ?movie__up_country <http://data.linkedmdb.org/movie/country_areaInSqKm> ?movie__up_country__down_country_areaInSqKm . }
        } """

        val sparqlstring = """
          SELECT ?movie ?movie__down_genre__down_film_genre_name ?movie__down_title
          WHERE {
          ?movie <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.linkedmdb.org/movie/film> .
          OPTIONAL { ?movie <http://data.linkedmdb.org/movie/genre> ?movie__down_genre . ?movie__down_genre <http://data.linkedmdb.org/movie/film_genre_name> ?movie__down_genre__down_film_genre_name . }
          OPTIONAL { ?movie <http://purl.org/dc/terms/title> ?movie__down_title . }

          } """

         */

    currentTime = System.nanoTime
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(sparqlString)
      .setCollapsByKey(sparqlFrameCollapse)
    val extractedFeaturesDf = sparqlFrame
      .transform(dataset)
      // .withColumnRenamed("actor(ListOf_NonCategorical_String)", "actor(ListOf_Categorical_String)")
      .persist()
    val extractedFeaturesDfSize = extractedFeaturesDf.count()
    extractedFeaturesDf.show(false)
    println(s"extractedFeaturesDfSize: $extractedFeaturesDfSize")
    val sparqlFrameTime = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed: ${sparqlFrameTime}")
    dataset.unpersist()

    println("\nSMART VECTOR ASSEMBLER")
    currentTime = System.nanoTime
    val labelColumnName = extractedFeaturesDf.columns.filter(_.contains(svaLabelColumn))(0)
    println(s"svaEntityColumn $svaEntityColumn svaLabelColumn $svaLabelColumn labelColumnName $labelColumnName")
    val smartVectorAssembler = new SmartVectorAssembler()
      .setEntityColumn(svaEntityColumn)
      // .setLabelColumn(labelColumnName)
      .setNullReplacement("string", "")
      .setNullReplacement("digit", -1)
      .setWord2VecSize(svaWord2VecSize)
      .setWord2VecMinCount(svaWord2VecMinCount)
      // .setWord2vecTrainingDfSizeRatio(svaWord2vecTrainingDfSizeRatio)
    val assembledDf: DataFrame = smartVectorAssembler
      .transform(extractedFeaturesDf)
      .persist()
    assembledDf.show(false)
    val assembledDfSize = assembledDf.count()
    println(f"assembled df has ${assembledDfSize} rows")
    val timeSmartVectorAssembler = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed: ${timeSmartVectorAssembler}")
    extractedFeaturesDf.unpersist()

    /* println("\nAPPLY Common SPARK MLlib Example Algorithm")
    val mlDf = assembledDf
      // .select(col("entityID"), explode(col("label")), col("features"))
      // .withColumnRenamed("col", "label")
    mlDf.show()
    currentTime = System.nanoTime
    /*
    Indoex Labels
     */
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(mlDf)
      .setHandleInvalid("skip")
    val mlDflabeledIndex = labelIndexer.transform(mlDf)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
      .setNumTrees(10)
    val model = rf.fit(mlDflabeledIndex.distinct())

    // Make predictions
    val predictions = model.transform(mlDflabeledIndex)
    // predictions.show(false)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labelsArray(0))

    val predictedLabelsDf = labelConverter
      .transform(predictions)
      .select("entityID", "label", "predictedLabel")

    predictedLabelsDf
      .show(false)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${(1.0 - accuracy)}")

    val timeSparkMLlib = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed: ${timeSparkMLlib}")

     */

    spark.stop()

    // write part
    val writePath: String = writeFolderPath + "DistRDF2ML_" +
      datetime
        .replace(":", "")
        .replace(" ", "") +
      ".txt"
    val writer = new PrintWriter(new File(writePath))
    writer.write(s"datetime: $datetime \n")
    writer.write(s"inputPath: $inputPath \n")
    writer.write(s"sparqlString: $sparqlString \n")
    writer.write(s"sparqlFrameCollapse: $sparqlFrameCollapse \n")
    writer.write(s"assembledDfSize: $assembledDfSize \n")
    writer.write(s"svaEntityColumn: $svaEntityColumn \n")
    writer.write(s"svaLabelColumn: $svaLabelColumn \n")
    writer.write(s"svaWord2VecSize: $svaWord2VecSize \n")
    writer.write(s"svaWord2VecMinCount: $svaWord2VecMinCount \n")
    writer.write(s"comments: $comments \n")
    writer.write(s"timeSparkSetup: $timeSparkSetup \n")
    writer.write(s"timeReadIn: $timeReadIn \n")
    writer.write(s"sparqlFrameTime: $sparqlFrameTime \n")
    writer.write(s"timeSmartVectorAssembler: $timeSmartVectorAssembler \n")
    // writer.write(s"timeSparkMLlib: $timeSparkMLlib \n")

    writer.close()
    Source.fromFile(writePath).foreach { x => print(x) }
  }
}
