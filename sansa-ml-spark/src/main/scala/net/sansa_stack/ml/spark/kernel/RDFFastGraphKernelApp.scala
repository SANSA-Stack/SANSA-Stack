package net.sansa_stack.ml.spark.kernel

import java.io.File

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.jena.graph
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object RDFFastGraphKernelApp {

  def main(args: Array[String]): Unit = {
    /*val taskNum: Int = scala.io.StdIn.readLine("Task Number?(1=Affiliation, 2=Lithogenesis, 3=Multi-contract, 4=Theme) ").toInt
    val depth: Int = scala.io.StdIn.readLine("Depth? ").toInt
    val algorithm: Int = scala.io.StdIn.readLine("Algorithm?(1=LogisticRegressionWithLBFGS, 2=RandomForest, 3=LogisticRegression) ").toInt
    val iteration: Int = scala.io.StdIn.readLine("How many iterations or folding on validation? ").toInt
	*/
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("fast graph kernel")
      .config("spark.driver.maxResultSize", "3g")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)
    
    // TEST
    // Minimal example, but some other unnecessary functions are still called
    val input = "src/main/resources/kernel/Lexicon_NamedRockUnit_t.nt"
    println("Reading to triples")
    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    println("TO RDD")
    val tripleRDD: TripleRDD = new TripleRDD(triples)
    println("Try faster version")
    val th0 = System.nanoTime
    tripleRDD.getTriples.filter(_.getPredicate.getURI == "http://data.bgs.ac.uk/ref/Lexicon/hasTheme")
      .foreach(f => Uri2Index.setInstanceAndLabel(f.getSubject.toString, f.getObject.toString))
    
    val filteredTripleRDD: TripleRDD = new TripleRDD(triples
      .filter(_.getPredicate.getURI != "http://data.bgs.ac.uk/ref/Lexicon/hasTheme")
    )

    val instanceDF = Uri2Index.getInstanceLabelsDF(sparkSession)
    //val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, filteredTripleRDD, instanceDF, 1)
    // The full computation of feature vectors (not yet prepared for ML/MLLIB) happens in computeFeatures2
    //rdfFastGraphKernel.computeFeatures2()
    //val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    //val tripleRDD: TripleRDD = new TripleRDD(triples)
    //val instanceDF = Uri2Index.getInstanceLabelsDF(sparkSession)
    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, tripleRDD, instanceDF, 1)
    val th1 = System.nanoTime
    val t0 = System.nanoTime
    val data = rdfFastGraphKernel.getMLLibLabeledPoints
    val t1 = System.nanoTime
    data.take(100).foreach(println(_))
    val tp0 = System.nanoTime
    predictLogisticRegressionMLLIB(data,2,1)
    val tp1 = System.nanoTime
    printTime("Overhead",th0,th1)
    printTime("FV",t0,t1)
    printTime("Prediction",tp0,tp1)
    
    /*val t0 = System.nanoTime

    if (taskNum == 1) {
      experimentAffiliationPrediction(sparkSession, depth, algorithm, iteration)
    }

    if (taskNum == 3) {
      experimentMultiContractPrediction(sparkSession, depth, algorithm, iteration)
    }

    if (taskNum == 4) {
      experimentThemePrediction(sparkSession, depth, algorithm, iteration)
    }

    printTime("Total: ", t0, System.nanoTime)
    println("taskNum: " + taskNum)
    println("depth: " + depth)
    println("algorithm: " + algorithm)
    println("iteration: " + iteration)*/
    sparkSession.stop
  }

  def printTime(title: String, t0: Long, t1: Long): Unit = {
    println(title)
    println("  Elapsed time: " + (t1 - t0) / 1e9d + " s")
  }


  def experimentAffiliationPrediction(sparkSession: SparkSession, depth: Int, algorithm: Int, iteration: Int): Unit = {
    //val input = "src/main/resources/kernel/aifb-fixed_complete4.nt"
    val input = "src/main/resources/kernel/aifb-fixed_no_schema4.nt"

    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val tripleRDD: TripleRDD = new TripleRDD(triples)

    val t0 = System.nanoTime
    // Note: it should be in Scala Iterable, to make sure setting unique indices
    tripleRDD.getTriples.filter(_.getPredicate.getURI == "http://swrc.ontoware.org/ontology#affiliation")
        .foreach(f => Uri2Index.setInstanceAndLabel(f.getSubject.toString, f.getObject.toString))
    tripleRDD.getTriples.filter(_.getPredicate.getURI == "http://swrc.ontoware.org/ontology#employs")
      .foreach(f => Uri2Index.setInstanceAndLabel(f.getObject.toString, f.getSubject.toString))
    val t1 = System.nanoTime

    // Note: remove triples that include prediction target
    val filteredTripleRDD: TripleRDD = new TripleRDD(triples
      .filter(_.getPredicate.getURI != "http://swrc.ontoware.org/ontology#affiliation")
      .filter(_.getPredicate.getURI != "http://swrc.ontoware.org/ontology#employs")
    )
    val t2 = System.nanoTime


    val instanceDF = Uri2Index.getInstanceLabelsDF(sparkSession)
//    instanceDF.groupBy("label").count().show()
//    +-----+-----+
//    |label|count|
//    +-----+-----+
//    |  0.0|   60|
//    |  1.0|   73|
//    |  2.0|   28|
//    |  3.0|   16|
//    +-----+-----+
    val t3 = System.nanoTime

    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, filteredTripleRDD, instanceDF, depth)
    val t4 = System.nanoTime

    printTime("Set Instance And Label", t0, t1)
    printTime("Filter Triple RDD", t1, t2)
    printTime("Get Instance Labels DF", t2, t3)
    printTime("Init RDFFastGraphKernel", t3, t4)

    processPrediction(rdfFastGraphKernel, 4, algorithm, iteration)
  }


  def experimentMultiContractPrediction(sparkSession: SparkSession, depth: Int, algorithm: Int, iteration: Int): Unit = {
    val input = "src/main/resources/kernel/LDMC_Task2_train.nt"


    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val tripleRDD: TripleRDD = new TripleRDD(triples)

    val t0 = System.nanoTime
    //"... http://example.com/multicontract ..."
    // it should be in Scala Iterable, to make sure setting unique indices
    tripleRDD.getTriples.filter(_.getPredicate.getURI == "http://example.com/multicontract")
      .foreach(f => Uri2Index.setInstanceAndLabel(f.getSubject.toString, f.getObject.toString))
    val t1 = System.nanoTime

    val filteredTripleRDD: TripleRDD = new TripleRDD(triples.filter(_.getPredicate.getURI != "http://example.com/multicontract"))

    val t2 = System.nanoTime

    val instanceDF = Uri2Index.getInstanceLabelsDF(sparkSession)
    // instanceDF.groupBy("label").count().show()
    // +-----+-----+
    // |label|count|
    // +-----+-----+
    // |  0.0|   40|
    // |  1.0|  168|
    // +-----+-----+
    val t3 = System.nanoTime

    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, filteredTripleRDD, instanceDF, depth)

    val t4 = System.nanoTime

    printTime("Set Instance And Label", t0, t1)
    printTime("Filter Triple RDD", t1, t2)
    printTime("Get Instance Labels DF", t2, t3)
    printTime("Init RDFFastGraphKernel", t3, t4)

    processPrediction(rdfFastGraphKernel, 2, algorithm, iteration)
  }


  def experimentThemePrediction(sparkSession: SparkSession, depth: Int, algorithm: Int, iteration: Int): Unit = {
    //val input = "src/main/resources/kernel/aifb-fixed_complete4.nt"
    val input = "src/main/resources/kernel/Lexicon_NamedRockUnit_t10.nt"


    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val tripleRDD: TripleRDD = new TripleRDD(triples)

    // it should be in Scala Iterable, to make sure setting unique indices
    val t0 = System.nanoTime
    tripleRDD.getTriples.filter(_.getPredicate.getURI == "http://data.bgs.ac.uk/ref/Lexicon/hasTheme")
      .foreach(f => Uri2Index.setInstanceAndLabel(f.getSubject.toString, f.getObject.toString))
    val t1 = System.nanoTime

    val filteredTripleRDD: TripleRDD = new TripleRDD(triples
      .filter(_.getPredicate.getURI != "http://data.bgs.ac.uk/ref/Lexicon/hasTheme")
    )
    val t2 = System.nanoTime

    val instanceDF = Uri2Index.getInstanceLabelsDF(sparkSession)
//    instanceDF.groupBy("label").count().show()
//    +-----+-----+
//    |label|count|
//    +-----+-----+
//    |  0.0| 1005|
//    |  1.0|  137|
//    +-----+-----+

    val t3 = System.nanoTime

    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, filteredTripleRDD, instanceDF, depth)

    val t4 = System.nanoTime()

    printTime("Set Instance And Label", t0, t1)
    printTime("Filter Triple RDD", t1, t2)
    printTime("Get Instance Labels DF", t2, t3)
    printTime("Init RDFFastGraphKernel", t3, t4)

    processPrediction(rdfFastGraphKernel, 2, algorithm, iteration)

  }


  def processPrediction(rdfFastGraphKernel: RDFFastGraphKernel, numClasses: Int = 2, algorithm:Int = 1, iteration: Int = 1): Unit = {
    val t0 = System.nanoTime
    var t1 = System.nanoTime
    if (algorithm == 1) {
      println("LogisticRegressionWithLBFGS")

      val data = rdfFastGraphKernel.getMLLibLabeledPoints
      data.cache()

      t1 = System.nanoTime
      printTime("Compute features", t0, t1)

      predictLogisticRegressionMLLIB(data, numClasses, iteration)
    } else if (algorithm == 2) {
      println("RandomForest")

      val data = rdfFastGraphKernel.getMLFeatureVectors
      data.cache()

      t1 = System.nanoTime
      printTime("Compute features", t0, t1)

      predictRandomForestML(data, iteration)
    } else if (algorithm == 3) {
      println ("LogisticRegression")

      val data = rdfFastGraphKernel.getMLFeatureVectors
      data.cache()

      t1 = System.nanoTime
      printTime("Compute features", t0, t1)

      predictLogisticRegressionML(data, iteration)
    }

    printTime("Training and Predict", t1, System.nanoTime())
  }


  def predictLogisticRegressionMLLIB(data: RDD[LabeledPoint], numClasses : Int = 2, maxIteration: Int = 0): Unit = {

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




  def predictRandomForestML(data: DataFrame, numFold: Int): Unit = {

    // Split data into training and test.
    val splits: Array[Dataset[Row]] = data.randomSplit(Array(0.8, 0.2))
    val training: Dataset[Row] = splits(0).cache()
    val test: Dataset[Row] = splits(1)

    println("training, test count", training.count(), test.count())


    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(rf))

    val paramGrid = new ParamGridBuilder().build() // No parameter search

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(numFold)

    val cvModel = cv.fit(training)
    val predictions = cvModel.transform(test)

    predictions.show(20)
    val rmse = evaluator.evaluate(predictions)
    println("RMSE: " + rmse)



  }

  def predictLogisticRegressionML(data: DataFrame, numFold: Int): Unit = {

    // Split data into training and test.
    val splits: Array[Dataset[Row]] = data.randomSplit(Array(0.8, 0.2))
    val training: Dataset[Row] = splits(0).cache()
    val test: Dataset[Row] = splits(1)

    println("training, test count", training.count(), test.count())


    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setLabelCol("label")
      .setFeaturesCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(lr))

    val paramGrid = new ParamGridBuilder().build() // No parameter search

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(numFold)

    val cvModel = cv.fit(training)
    val predictions = cvModel.transform(test)

    predictions.show(20)
    val rmse = evaluator.evaluate(predictions)
    println("RMSE: " + rmse)



  }


}