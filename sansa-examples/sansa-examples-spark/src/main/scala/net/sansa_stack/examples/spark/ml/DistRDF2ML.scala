package net.sansa_stack.examples.spark.ml

import java.io.{File, PrintWriter}
import java.util.Calendar

import net.sansa_stack.ml.spark.featureExtraction.{FeatureExtractingSparqlGenerator, SmartVectorAssembler, SparqlFrame}
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.rdf.common.io.riot.error.{ErrorParseMode, WarningParseMode}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.sys.JenaSystem
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object DistRDF2ML {
  def main(args: Array[String]): Unit = {

    // readIn
    val inputPath: String = args(0)

    // sparqlFrame
    val sparqlString: String = args(1)
    val sparqlFrameCollapse: Boolean = true

    // smartVector assembler
    val svaEntityColumn: String = "movie"
    val svaLabelColumn: String = "movie__down_genre__down_film_genre_name(Single_Categorical_String)"
    val svaWord2VecSize: Int = 5
    val svaWord2VecMinCount: Int = 1

    // datetime
    val datetime: String = Calendar.getInstance().getTime().toString

    // write
    val writeFolderPath: String = args(2)
    val writePath: String = writeFolderPath + "DistRDF2ML_" +
      datetime
        .replace(":", "")
        .replace(" ", "") +
      ".txt"

    // write part
    val writer = new PrintWriter(new File(writePath))
    writer.write(s"datetime: $datetime \n")
    writer.write(s"inputPath: $inputPath \n")
    writer.write(s"sparqlString: $sparqlString \n")
    writer.write(s"sparqlFrameCollapse: $sparqlFrameCollapse \n")
    writer.write(s"svaEntityColumn: $svaEntityColumn \n")
    writer.write(s"svaLabelColumn: $svaLabelColumn \n")
    writer.write(s"svaWord2VecSize: $svaWord2VecSize \n")
    writer.write(s"svaWord2VecMinCount: $svaWord2VecMinCount \n")

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
    writer.write(s"timeSparkSetup: $timeSparkSetup \n")
    println(spark.sparkContext.getConf)

    println("\nREAD IN DATA")
    currentTime = System.nanoTime
    val dataset = {
      NTripleReader.load(
        spark,
        inputPath,
        stopOnBadTerm = ErrorParseMode.SKIP,
        stopOnWarnings = WarningParseMode.IGNORE
      ).toDS().cache()
    }
    println(f"\ndata consists of ${dataset.count()} triples")
    dataset.take(n = 10).foreach(println(_))
    val timeReadIn = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed: ${timeReadIn}")
    writer.write(s"timeSparkSetup: $timeSparkSetup \n")

    println("\nFEATURE EXTRACTION OVER SPARQL")
    currentTime = System.nanoTime
    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(sparqlString)
      .setCollapsByKey(sparqlFrameCollapse)
    val extractedFeaturesDf = sparqlFrame.transform(dataset)
    extractedFeaturesDf.show(false)
    val sparqlFrameTime = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed: ${sparqlFrameTime}")
    writer.write(s"sparqlFrameTime: $sparqlFrameTime \n")

    println("\nSMART VECTOR ASSEMBLER")
    currentTime = System.nanoTime
    val smartVectorAssembler = new SmartVectorAssembler()
      .setEntityColumn(svaEntityColumn)
      .setLabelColumn(svaLabelColumn)
      .setNullReplacement("string", "null")
      .setNullReplacement("digit", -1)
      .setWord2VecSize(svaWord2VecSize)
      .setWord2VecMinCount(svaWord2VecMinCount)

     val assembledDf: DataFrame = smartVectorAssembler
       .transform(extractedFeaturesDf)
       .cache()
    assembledDf.show(false)
    println(f"assembled df has ${assembledDf.count()} rows")
    val timeSmartVectorAssembler = (System.nanoTime - currentTime) / 1e9d
    println(f"\ntime needed: ${timeSmartVectorAssembler}")
    writer.write(s"timeSmartVectorAssembler: $timeSmartVectorAssembler \n")

    println("\nAPPLY Common SPARK MLlib Example Algorithm")
    currentTime = System.nanoTime
    /*
    Indoex Labels
     */
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(assembledDf).setHandleInvalid("skip")
    val assembledDflabeledIndex = labelIndexer.transform(assembledDf)
    // assembledDflabeledIndex.show(false)

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

    labelConverter
      .transform(predictions)
      .select("entityID", "label", "predictedLabel")
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
    writer.write(s"timeSparkMLlib: $timeSparkMLlib \n")

    spark.stop()

    writer.close()
    Source.fromFile(writePath).foreach { x => print(x) }

  }
}