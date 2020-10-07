package net.sansa_stack.ml.spark.kernel

import org.apache.jena.graph
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.classification.{ LogisticRegressionModel, LogisticRegressionWithLBFGS }
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._

object RDFFastTreeGraphKernelUtil {

  def triplesToDF(
    sparkSession: SparkSession,
    triples: RDD[graph.Triple],
    subjectColName: String = "subject",
    predicateColName: String = "predicate",
    objectColName: String = "object"): DataFrame = {
    import sparkSession.implicits._

    triples.map(f => (f.getSubject.toString, f.getPredicate.toString, f.getObject.toString))
      .toDF(subjectColName, predicateColName, objectColName)
  }

  def getInstanceAndLabelDF(
    filteredTripleDF: DataFrame,
    subjectColName: String = "subject",
    objectColName: String = "object"): DataFrame = {
    /*
      root
      |-- instance: string (nullable = true)
      |-- label: double (nullable = true)
    */

    val df = filteredTripleDF.select(subjectColName, objectColName).distinct()

    val indexer = new StringIndexer()
      .setInputCol(objectColName)
      .setOutputCol("label")
      .fit(df)
    val indexedDF = indexer.transform(df).drop(objectColName)
      .groupBy(subjectColName)
      .agg(max("label") as "label")
      .toDF("instance", "label")

    indexedDF
  }

  def predictLogisticRegressionMLLIB(data: RDD[LabeledPoint], numClasses: Int = 2, maxIteration: Int = 5): Unit = {
    val t0 = System.nanoTime

    data.cache()
    println("data count", data.count())

    val t1 = System.nanoTime

    def trainAndValidate(data: RDD[LabeledPoint], seed: Long): (LogisticRegressionModel, Double) = {
      val splits = data.randomSplit(Array(0.9, 0.1), seed)
      val training = splits(0).cache()
      val validation = splits(1)
      val model = new LogisticRegressionWithLBFGS().setNumClasses(numClasses).run(training)

      val predictions = validation.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      val metrics = new MulticlassMetrics(predictions)
      val accuracy = metrics.accuracy

      (model, accuracy)
    }

    var sumOfAccuracy = 0.0

    for (seed <- 1 to maxIteration) {
      val (model, accuracy) = trainAndValidate(data, seed)
      //      println(accuracy)
      sumOfAccuracy += accuracy
    }

    val t2 = System.nanoTime

    // score the model on test data.
    println("Average Accuracy: " + sumOfAccuracy / maxIteration)

    RDFFastTreeGraphKernelUtil.printTime("Feature Computation/Read", t0, t1)
    RDFFastTreeGraphKernelUtil.printTime("Model learning/testing", t1, t2)
  }

  def printTime(title: String, t0: Long, t1: Long): Unit = {
    println(title + ": " + (t1 - t0) / 1e9d + " s")
  }

}
