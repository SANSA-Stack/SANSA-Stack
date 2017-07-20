package net.sansa_stack.ml.spark.kernel

import java.io.File

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.jena.graph
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS,LogisticRegressionWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.tuning.CrossValidator

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}


object RDFFastGraphKernelApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("fast graph kernel")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    // Task 1
//    experimentAffiliationPrediction(sparkSession)

    // Task 3
//    experimentMultiContractPrediction(sparkSession)

    // Task 4
//    experimentThemePrediction(sparkSession)

    sparkSession.stop
  }



  def experimentAffiliationPrediction(sparkSession: SparkSession): Unit = {
    //val input = "src/main/resources/kernel/aifb-fixed_complete4.nt"
    val input = "src/main/resources/kernel/aifb-fixed_no_schema4.nt"

    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val tripleRDD: TripleRDD = new TripleRDD(triples)

    // Note: it should be in Scala Iterable, to make sure setting unique indices
    tripleRDD.getTriples.filter(_.getPredicate.getURI == "http://swrc.ontoware.org/ontology#affiliation")
        .foreach(f => Uri2Index.setInstanceAndLabel(f.getSubject.toString, f.getObject.toString))
    tripleRDD.getTriples.filter(_.getPredicate.getURI == "http://swrc.ontoware.org/ontology#employs")
      .foreach(f => Uri2Index.setInstanceAndLabel(f.getObject.toString, f.getSubject.toString))

    // Note: remove triples that include prediction target
    val filteredTripleRDD: TripleRDD = new TripleRDD(triples
      .filter(_.getPredicate.getURI != "http://swrc.ontoware.org/ontology#affiliation")
      .filter(_.getPredicate.getURI != "http://swrc.ontoware.org/ontology#employs")
    )


    val instanceDF = Uri2Index.getInstanceLabelsDF(sparkSession)
//    instanceDF.show(20)
//    instanceDF.printSchema()


    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, filteredTripleRDD, instanceDF, 2)
//    rdfFastGraphKernel.computeFeatures()
    //val data = rdfFastGraphKernel.getMLFeatureVectors
    //data.show(1000)
    val data = rdfFastGraphKernel.getMLLibLabeledPoints
    //println("MLLIB")
    //data.foreach(println(_))
    data.cache()
    var testerr = 0.0
    for(seed <- 1 to 10){
      testerr += predictMultiClassProcessMLLIB(data,seed)
    }
    println("AVERAGE ERROR")
    println(testerr/10)
  }
  
  def experimentThemePrediction(sparkSession: SparkSession): Unit = {
   //val input = "src/main/resources/kernel/aifb-fixed_complete4.nt"
    val input = "src/main/resources/kernel/Lexicon_NamedRockUnit_t10.nt"


    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val tripleRDD: TripleRDD = new TripleRDD(triples)

    // it should be in Scala Iterable, to make sure setting unique indices
    println("Set Instance And Label")
    val t0 = System.nanoTime()
    tripleRDD.getTriples.filter(_.getPredicate.getURI == "http://data.bgs.ac.uk/ref/Lexicon/hasTheme")
        .foreach(f => Uri2Index.setInstanceAndLabel(f.getSubject.toString, f.getObject.toString))
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    println("Filter Triple RDD")
    val filteredTripleRDD: TripleRDD = new TripleRDD(triples
      .filter(_.getPredicate.getURI != "http://data.bgs.ac.uk/ref/Lexicon/hasTheme")
    )
    val t2 = System.nanoTime()
    println("Elapsed time: " + (t2 - t1) + "ns")
    println("Get Instance Labels DF")
    val instanceDF = Uri2Index.getInstanceLabelsDF(sparkSession)
    val t3 = System.nanoTime()
    println("Elapsed time: " + (t3 - t2) + "ns")
    println("Compute Features")
    Uri2Index.label2uri.foreach(println(_))
    println("now u2l")
    Uri2Index.uri2label.foreach(println(_))
    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, filteredTripleRDD, instanceDF, 2)
    rdfFastGraphKernel.computeFeatures()
    val t4 = System.nanoTime()
    println("Elapsed time: " + (t4 - t3) + "ns")
    //val data = rdfFastGraphKernel.getMLFeatureVectors
    //data.show(10000)
    val data = rdfFastGraphKernel.getMLLibLabeledPoints
    //println("MLLIB")
    //data.foreach(println(_))
    var testerr = 0.0
    // Changed Loop to "1 to 1" for fast tests
    for(seed <- 1 to 1){
      testerr += predictMultiClassProcessMLLIB(data,2,seed)
    }
    val t5 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    println("Elapsed time: " + (t2 - t1) + "ns")
    println("Elapsed time: " + (t3 - t2) + "ns")
    println("Elapsed time: " + (t4 - t3) + "ns")
    println("Elapsed time: " + (t5 - t4) + "ns")
    println("AVERAGE ERROR")
    println(testerr/1)
    println("TOTAL TIME: " + (t5-t0) + "ns")
  }

  def predictMultiClassProcessMLLIB(data: RDD[LabeledPoint], NumClasses : Int = 2, seed: Long = 0): Double = {
    // Some stuff for SVM:

    // Split data into training and test.
    val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.9, 0.1), seed)
    val training: RDD[LabeledPoint] = splits(0)
    val test: RDD[LabeledPoint] = splits(1)

    println("training, test count", training.count(), test.count())


    //val classifier = new LogisticRegression()
    //  .setMaxIter(10)
    //  .setTol(1E-6)
    //  .setFitIntercept(true)
    
    //val ovrModel = new LogisticRegressionWithLBFGS().setNumClasses(NumClasses).run(training)
    val ovrModel = new LogisticRegressionWithSGD().run(training)

    // instantiate the One Vs Rest Classifier.
    //val ovr = new OneVsRest().setClassifier(classifier)

    // train the multiclass model.
    //val ovrModel = ovr.fit(training)

    // score the model on test data.
    //val predictions = ovrModel.transform(test)
    val predictions = test.map{ point =>
      val prediction = ovrModel.predict(point.features)
      (point.label, prediction)
    }
    //println("show test")
    //test.show(20)
    println("show predictions")
    val trainErr = predictions.filter(f => f._1 != f._2).count.toDouble/test.count
    //predictions.foreach(println(_))
    println(trainErr)
    println(predictions.filter(f => f._1 != f._2).count.toDouble)
    println(test.count)
    // obtain evaluator.
    //val evaluator = new MulticlassClassificationEvaluator()
    //  .setMetricName("accuracy")

    // compute the classification error on test data.
    //val accuracy = evaluator.evaluate(predictions)
    //println(s"Test Error = ${1 - accuracy}")

    trainErr
  }





  def experimentMultiContractPrediction(sparkSession: SparkSession): Unit = {
    val input = "src/main/resources/kernel/LDMC_Task2_train.nt"


    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val tripleRDD: TripleRDD = new TripleRDD(triples)

    //"... http://example.com/multicontract ..."
    // it should be in Scala Iterable, to make sure setting unique indices
    tripleRDD.getTriples.filter(_.getPredicate.getURI == "http://example.com/multicontract")
      .foreach(f => Uri2Index.setInstanceAndLabel(f.getSubject.toString, f.getObject.toString))

    val instanceDF = Uri2Index.getInstanceLabelsDF(sparkSession)
    // instanceDF.groupBy("label").count().show()
    // +-----+-----+
    // |label|count|
    // +-----+-----+
    // |  0.0|   40|
    // |  1.0|  168|
    // +-----+-----+



    val filteredTripleRDD: TripleRDD = new TripleRDD(triples.filter(_.getPredicate.getURI != "http://example.com/multicontract"))

    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, filteredTripleRDD, instanceDF, 2)
//    val data = rdfFastGraphKernel.getMLFeatureVectors
    val data = rdfFastGraphKernel.computeFeatures().collect()

//    randomForestPredict(data)


  }

  def randomForestPredict(data: DataFrame): Unit = {
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

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
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("Learned classification forest model:\n" + rfModel.toDebugString)

  }




/*
  def predictMultiClassProcess(data: DataFrame): Unit = {
    // Some stuff for SVM:

    // Split data into training and test.
    val splits: Array[Dataset[Row]] = data.randomSplit(Array(0.4, 0.1, 0.4, 0.1), seed = 11L)
    val training: Dataset[Row] = splits(0)
    val test: Dataset[Row] = splits(1)

    println("training, test count", training.count(), test.count())


    //val classifier = new LogisticRegression()
    //  .setMaxIter(10)
    //  .setTol(1E-6)
    //  .setFitIntercept(true)
    
    val ovrModel = new LogisticRegressionWithLBFGS().setNumClasses(5).run(training)


    // instantiate the One Vs Rest Classifier.
    //val ovr = new OneVsRest().setClassifier(classifier)

    // train the multiclass model.
    //val ovrModel = ovr.fit(training)

    // score the model on test data.
    //val predictions = ovrModel.transform(test)
    val predictions = ovrModel.transform(training)
    println("show test")
    test.show(20)
    println("show predictions")
    predictions.show(200)

    // obtain evaluator.
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    // compute the classification error on test data.
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1 - accuracy}")


  }
*/
}

