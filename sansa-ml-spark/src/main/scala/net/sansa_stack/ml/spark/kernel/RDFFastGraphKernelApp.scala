package net.sansa_stack.ml.spark.kernel

import java.io.File

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.jena.graph
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDFFastGraphKernelApp {

  def main(args: Array[String]): Unit = {
    val taskNum: Int = scala.io.StdIn.readLine("Task Number?(1=Affiliation, 2=Lithogenesis, 3=Multi-contract, 4=Theme) ").toInt
    val iteration: Int = scala.io.StdIn.readLine("How many iterations or folding on validation? ").toInt

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("fast graph kernel")
      .config("spark.driver.maxResultSize", "3g")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)


    val t0 = System.nanoTime

    if (taskNum == 1) {
      experimentAffiliationPrediction(sparkSession, iteration)
    }

    if (taskNum == 3) {
      experimentMultiContractPrediction(sparkSession, iteration)
    }

    if (taskNum == 4) {
      experimentThemePrediction(sparkSession, iteration)
    }

    printTime("Total: ", t0, System.nanoTime)
    println("taskNum: " + taskNum)
    println("iteration: " + iteration)
    sparkSession.stop
  }

  def printTime(title: String, t0: Long, t1: Long): Unit = {
    println(title)
    println("  Elapsed time: " + (t1 - t0) / 1e9d + " s")
  }


  def experimentAffiliationPrediction(sparkSession: SparkSession, iteration: Int): Unit = {
    //val input = "src/main/resources/kernel/aifb-fixed_complete4.nt"
    val input = "src/main/resources/kernel/aifb-fixed_no_schema4.nt"

    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val tripleRDD: TripleRDD = new TripleRDD(triples)

    val t0 = System.nanoTime

    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, tripleRDD, "http://swrc.ontoware.org/ontology#affiliation")
    val data = rdfFastGraphKernel.getMLLibLabeledPoints

    val t1 = System.nanoTime

    predictLogisticRegressionMLLIB(data,2,iteration)

    val t2 = System.nanoTime

    printTime("Init RDFFastGraphKernel", t0, t1)
    printTime("Run Prediction", t1, t2)



  }


  def experimentMultiContractPrediction(sparkSession: SparkSession, iteration: Int): Unit = {
    val input = "src/main/resources/kernel/LDMC_Task2_train.nt"


    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val tripleRDD: TripleRDD = new TripleRDD(triples)

    val t0 = System.nanoTime

    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, tripleRDD, "http://example.com/multicontract")
    val data = rdfFastGraphKernel.getMLLibLabeledPoints

    val t1 = System.nanoTime

    predictLogisticRegressionMLLIB(data,2,iteration)

    val t2 = System.nanoTime

    printTime("Init RDFFastGraphKernel", t0, t1)
    printTime("Run Prediction", t1, t2)

  }


  def experimentThemePrediction(sparkSession: SparkSession, iteration: Int): Unit = {

    val input = "src/main/resources/kernel/Lexicon_NamedRockUnit_t.nt"

    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val tripleRDD: TripleRDD = new TripleRDD(triples)


    val t0 = System.nanoTime

    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, tripleRDD, "http://data.bgs.ac.uk/ref/Lexicon/hasTheme")
    val data = rdfFastGraphKernel.getMLLibLabeledPoints

    val t1 = System.nanoTime

    predictLogisticRegressionMLLIB(data,2,iteration)

    val t2 = System.nanoTime

    printTime("Init RDFFastGraphKernel", t0, t1)
    printTime("Run Prediction", t1, t2)

  }


  def predictLogisticRegressionMLLIB(data: RDD[LabeledPoint], numClasses : Int = 2, maxIteration: Int = 1): Unit = {

    // Split data into training and test.
    val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.8, 0.2))
    val training: RDD[LabeledPoint] = splits(0).cache()
    val test: RDD[LabeledPoint] = splits(1)

    println("training, test count", training.count(), test.count())

    def trainAndValidate(data: RDD[LabeledPoint], seed: Long): (LogisticRegressionModel, Double) = {
      val splits = data.randomSplit(Array(0.9, 0.1), seed)
      val training = splits(0)
      val validation = splits(1)
      val model = new LogisticRegressionWithLBFGS().setNumClasses(numClasses).run(training)

      val predictions = validation.map{ point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      val metrics = new MulticlassMetrics(predictions)
      val accuracy = metrics.accuracy

      (model, accuracy)
    }

    var (bestModel, bestAccuracy) = trainAndValidate(training, 1)

    for ( seed <- 2 to maxIteration ) {
      val (model, accuracy) = trainAndValidate(training, seed)
      if (accuracy > bestAccuracy) {
        bestModel = model
        bestAccuracy = accuracy
      }
    }

    val predictions = test.map{ point =>
      val prediction = bestModel.predict(point.features)
      (point.label, prediction)
    }
    val metrics = new MulticlassMetrics(predictions)
    val accuracy = metrics.accuracy


    // score the model on test data.
    println("show predictions")
    predictions.foreach(println(_))
    println("accuracy: " + accuracy)

    val trainErr = predictions.filter(f => f._1 != f._2).count.toDouble/test.count
    println("trainErr", trainErr)
    println("numerator", predictions.filter(f => f._1 != f._2).count.toDouble)
    println("denominator", test.count)

  }


}