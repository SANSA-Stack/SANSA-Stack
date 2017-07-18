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

import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.tuning.CrossValidator


object RDFFastGraphKernelApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("fast graph kernel")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    experimentAffiliationPrediction(sparkSession)


    sparkSession.stop
  }



  def experimentAffiliationPrediction(sparkSession: SparkSession): Unit = {
   //val input = "src/main/resources/kernel/aifb-fixed_complete4.nt"
   val input = "src/main/resources/kernel/aifb-fixed_no_schema4.nt"


    val triples: RDD[graph.Triple] = NTripleReader.load(sparkSession, new File(input))
    val tripleRDD: TripleRDD = new TripleRDD(triples)

    // it should be in Scala Iterable, to make sure setting unique indices
    tripleRDD.getTriples.filter(_.getPredicate.getURI == "http://swrc.ontoware.org/ontology#affiliation")
        .foreach(f => Uri2Index.setInstanceAndLabel(f.getSubject.toString, f.getObject.toString))
    tripleRDD.getTriples.filter(_.getPredicate.getURI == "http://swrc.ontoware.org/ontology#employs")
      .foreach(f => Uri2Index.setInstanceAndLabel(f.getObject.toString, f.getSubject.toString))

    val filteredTripleRDD: TripleRDD = new TripleRDD(triples
      .filter(_.getPredicate.getURI != "http://swrc.ontoware.org/ontology#affiliation")
      .filter(_.getPredicate.getURI != "http://swrc.ontoware.org/ontology#employs")
    )


    // TODO: remove instances which belongs the least class
    val instanceDF = Uri2Index.getInstanceLabelsDF(sparkSession)
//    instanceDF.show(20)
//    instanceDF.printSchema()


    //val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, tripleRDD, instanceDF, 2)
    val rdfFastGraphKernel = RDFFastGraphKernel(sparkSession, filteredTripleRDD, instanceDF, 2)
    rdfFastGraphKernel.computeFeatures()
    //val data = rdfFastGraphKernel.getMLFeatureVectors
    //data.show(1000)
    val data = rdfFastGraphKernel.getMLLibLabeledPoints
    //println("MLLIB")
    //data.foreach(println(_))
    var testerr = 0.0
    for(seed <- 1 to 10){
      testerr += predictMultiClassProcessMLLIB(data,seed)
    }
    println("AVERAGE ERROR")
    println(testerr/10)
  }

def predictMultiClassProcessMLLIB(data: RDD[LabeledPoint], seed: Long = 0): Double = {
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
    
    val ovrModel = new LogisticRegressionWithLBFGS().setNumClasses(4).run(training)


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
    predictions.foreach(println(_))
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