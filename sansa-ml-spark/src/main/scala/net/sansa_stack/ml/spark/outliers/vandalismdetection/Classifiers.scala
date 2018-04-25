package net.sansa_stack.ml.spark.outliers.vandalismdetection

import org.apache.spark.{ SparkContext, RangePartitioner }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ DoubleType, StringType, IntegerType, StructField, StructType }
import org.apache.spark.ml.linalg.{ Vector, Vectors }
import org.apache.spark.ml.classification.{ GBTClassificationModel, GBTClassifier }
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import scala.collection.mutable
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer }
import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.Pipeline

class Classifiers extends Serializable {

  //ok -----
  def RandomForestClassifer(DF: DataFrame, sqlContext: SQLContext): Unit = {

    DF.registerTempTable("DB")
    val Data = sqlContext.sql("select Rid, features, FinalROLLBACK_REVERTED  from DB")

    val labelIndexer = new StringIndexer().setInputCol("FinalROLLBACK_REVERTED").setOutputCol("indexedLabel").fit(Data)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(Data)

    val Array(trainingData, testData) = Data.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestClassifier().setImpurity("gini").setMaxDepth(3).setNumTrees(20).setFeatureSubsetStrategy("auto").setSeed(5043).setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures") //.setNumTrees(20)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val model_New = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model_New.transform(testData)

    // Select example rows to display.
    val finlaPrediction = predictions.select("Rid", "features", "FinalROLLBACK_REVERTED", "predictedLabel")
    predictions.show()

    //Case1 : BinaryClassificationEvaluator:OK ------------------------------------------------------
    val binaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol("indexedLabel").setRawPredictionCol("rawPrediction")
    var results = 0.0
    def printlnMetricCAse1(metricName: String): Unit = {

      results = binaryClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      println(metricName + " = " + results)
    }
    printlnMetricCAse1("areaUnderROC")
    printlnMetricCAse1("areaUnderPR")

    // Case 2: MulticlassClassificationEvaluator:OK -----------------------------------------------------
    //Select (prediction, true label) and compute test error.
    val MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")

    def printlnMetricCase2(metricName: String): Unit = {
      println(metricName + " = " + MulticlassClassificationEvaluator.setMetricName(metricName).evaluate(predictions))
    }
    printlnMetricCase2("accuracy")
    printlnMetricCase2("weightedPrecision")
    printlnMetricCase2("weightedRecall")
    //
  }

  //ok------
  def DecisionTreeClassifier(DF: DataFrame, sqlContext: SQLContext): Unit = {

    DF.registerTempTable("DB")
    val Data = sqlContext.sql("select Rid, features, FinalROLLBACK_REVERTED  from DB")

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer().setInputCol("FinalROLLBACK_REVERTED").setOutputCol("indexedLabel").fit(Data)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(Data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = Data.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    // Train model. This also runs the indexers.
    val modelxx = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = modelxx.transform(testData)

    // Select example rows to display.
    val finlaPrediction = predictions.select("Rid", "features", "FinalROLLBACK_REVERTED", "predictedLabel")

    //Case1 : BinaryClassificationEvaluator:----------------------------------------------------------
    val binaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol("indexedLabel").setRawPredictionCol("rawPrediction")
    def printlnMetricCAse1(metricName: String): Unit = {

      println(metricName + " = " + binaryClassificationEvaluator.setMetricName(metricName).evaluate(predictions))
    }
    printlnMetricCAse1("areaUnderROC")
    printlnMetricCAse1("areaUnderPR")

    //Case 2: MulticlassClassificationEvaluator:-----------------------------------------------------
    //Select (prediction, true label) and compute test error.
    val MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")

    def printlnMetricCase2(metricName: String): Unit = {
      println(metricName + " = " + MulticlassClassificationEvaluator.setMetricName(metricName).evaluate(predictions))
    }
    printlnMetricCase2("accuracy")
    printlnMetricCase2("weightedPrecision")
    printlnMetricCase2("weightedRecall")

  }

  // Ok --------
  def LogisticRegrision(DF: DataFrame, sqlContext: SQLContext): Unit = {

    DF.registerTempTable("DB")

    val Data = sqlContext.sql("select Rid, features, FinalROLLBACK_REVERTED as label from DB") // for logistic regrision

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(Data)

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(Data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = Data.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    //val gbt = new GBTClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")//.setMaxIter(10)

    val mlr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setFamily("multinomial")

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, mlr, labelConverter))

    // Train model. This also runs the indexers.
    val modelxx = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = modelxx.transform(testData)

    // Select example rows to display.
    val finlaPrediction = predictions.select("Rid", "features", "label", "predictedLabel")

    predictions.show()

    //Case1 : BinaryClassificationEvaluator:----------------------------------------------------------
    val binaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol("indexedLabel").setRawPredictionCol("rawPrediction")
    var results = 0.0
    def printlnMetricCase1(metricName: String): Unit = {

      results = binaryClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      println(metricName + " = " + results)
    }
    printlnMetricCase1("areaUnderROC")
    printlnMetricCase1("areaUnderPR")

    //Case 2: MulticlassClassificationEvaluator:-----------------------------------------------------
    //Select (prediction, true label) and compute test error.
    val MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")

    def printlnMetricCase2(metricName: String): Unit = {
      println(metricName + " = " + MulticlassClassificationEvaluator.setMetricName(metricName).evaluate(predictions))
    }
    printlnMetricCase2("accuracy")
    printlnMetricCase2("weightedPrecision")
    printlnMetricCase2("weightedRecall")

  }
  // OK-----
  def GradientBoostedTree(DF: DataFrame, sqlContext: SQLContext): Unit = {

    DF.registerTempTable("DB")
    val Data = sqlContext.sql("select Rid, features, FinalROLLBACK_REVERTED  from DB").cache()

    val labelIndexer = new StringIndexer().setInputCol("FinalROLLBACK_REVERTED").setOutputCol("indexedLabel").fit(Data)

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(Data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = Data.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    val gbt = new GBTClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures") //.setMaxIter(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

    // Train model. This also runs the indexers.
    val modelxx = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = modelxx.transform(testData)

    // Select example rows to display.

    //Case1 : BinaryClassificationEvaluator:----------------------------------------------------------

    var predictionsRDD = predictions.select("prediction", "FinalROLLBACK_REVERTED").rdd
    var predictionAndLabels = predictionsRDD.map { row => (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Double]) }

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    println("Area under ROC = " + metrics.areaUnderROC())
    println("Area under PR = " + metrics.areaUnderPR())

    //Case 2: MulticlassClassificationEvaluator:-----------------------------------------------------
    //Select (prediction, true label) and compute test error.
    val MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")

    def printlnMetric(metricName: String): Unit = {
      println(metricName + " = " + MulticlassClassificationEvaluator.setMetricName(metricName).evaluate(predictions))
    }
    printlnMetric("accuracy")
    printlnMetric("weightedPrecision")
    printlnMetric("weightedRecall")

  }

  //Ok------------
  def MultilayerPerceptronClassifier(DF: DataFrame, sqlContext: SQLContext): Unit = {

    DF.registerTempTable("DB")

    val Data = sqlContext.sql("select Rid, features, FinalROLLBACK_REVERTED as label from DB")

    val Array(trainingData, testData) = Data.randomSplit(Array(0.7, 0.3))

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(Data)

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(Data)

    val layers = Array[Int](100, 5, 4, 2)

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(100)

    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, trainer, labelConverter))

    // train the model
    val modelxx = pipeline.fit(trainingData)

    // compute accuracy on the test set
    val predictions = modelxx.transform(testData)

    // predictions.show()

    //Case1 : BinaryClassificationEvaluator:----------------------------------------------------------
    var predictionsDF = predictions.select("prediction", "label")
    var predictionsRDD = predictions.select("prediction", "label").rdd
    var predictionAndLabels = predictionsRDD.map { row => (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Double]) }

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    println("Area under ROC = " + metrics.areaUnderROC())
    println("Area under PR = " + metrics.areaUnderPR())

    //Case 2: MulticlassClassificationEvaluator:-----------------------------------------------------
    val accuracyevaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
    val weightedPrecisionevaluator = new MulticlassClassificationEvaluator().setMetricName("weightedPrecision")
    val weightedRecallevaluator = new MulticlassClassificationEvaluator().setMetricName("weightedRecall")

    println("Accuracy = " + accuracyevaluator.evaluate(predictionsDF))
    println("weightedPrecision = " + weightedPrecisionevaluator.evaluate(predictionsDF))
    println("weightedRecall = " + weightedRecallevaluator.evaluate(predictionsDF))

  }

}