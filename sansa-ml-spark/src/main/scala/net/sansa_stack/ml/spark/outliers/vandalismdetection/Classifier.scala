package net.sansa_stack.ml.spark.outliers.vandalismdetection

import java.io.{ File, IOException }
import java.text.SimpleDateFormat
import java.util.{ Calendar, Date }

import scala.collection.mutable

import org.apache.commons.io.FileUtils
import org.apache.spark.{ RangePartitioner, SparkContext }
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{ DecisionTreeClassificationModel, DecisionTreeClassifier, GBTClassificationModel, GBTClassifier, LogisticRegression, MultilayerPerceptronClassifier, RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.evaluation.{ BinaryClassificationEvaluator, MulticlassClassificationEvaluator }
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer }
import org.apache.spark.ml.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD }
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StringType, StructField, StructType }

object Classifier extends Serializable {

  // 1. Random Forest Classifer
  def randomForestClassifer(train: DataFrame, test: DataFrame, spark: SparkSession): String = {

    train.createOrReplaceTempView("DB1")
    test.createOrReplaceTempView("DB2")

    val TrainingData = spark.sql("select Rid, features,FinalROLLBACK_REVERTED  from DB1")
    val TestingData = spark.sql("select Rid, features, FinalROLLBACK_REVERTED  from DB2")

    val labelIndexer = new StringIndexer().setInputCol("FinalROLLBACK_REVERTED").setOutputCol("indexedLabel").fit(TrainingData)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(TrainingData)

    // Train a RandomForest model.
    val rf = new RandomForestClassifier().setImpurity("gini").setMaxDepth(3).setNumTrees(20).setFeatureSubsetStrategy("auto").setSeed(5043)
      .setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures") // .setNumTrees(20)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val model_New = pipeline.fit(TrainingData)

    // Make predictions.
    val predictions = model_New.transform(TestingData)

    // Select example rows to display.
    val finlaPrediction = predictions.select("Rid", "features", "FinalROLLBACK_REVERTED", "predictedLabel")
    predictions.show()

    // Case1 : BinaryClassificationEvaluator:OK ------------------------------------------------------
    val binaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol("indexedLabel").setRawPredictionCol("rawPrediction")
    var results1 = 0.0
    def printlnMetricCAse1(metricName: String): Double = {

      results1 = binaryClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      println(metricName + " = " + results1)
      results1
    }
    val ROC = printlnMetricCAse1("areaUnderROC")
    val PR = printlnMetricCAse1("areaUnderPR")

    // Case 2: MulticlassClassificationEvaluator:OK -----------------------------------------------------
    // Select (prediction, true label) and compute test error.
    val MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
    var results2 = 0.0

    def printlnMetricCase2(metricName: String): Double = {
      results2 = MulticlassClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      println(metricName + " = " + results2)
      results2
    }
    val accuracy = printlnMetricCase2("accuracy")
    val Precision = printlnMetricCase2("weightedPrecision")
    val Recall = printlnMetricCase2("weightedRecall")

    val finalResult = "ROC=" + ROC.toString() + "|" + "PR=" + PR.toString() + "|" + "accuracy=" + accuracy.toString() + "|" + "Precision=" + Precision.toString() + "|" + "Recall=" + Recall.toString()
    finalResult

  }
  // 2. Decision Tree Classifier
  def decisionTreeClassifier(train: DataFrame, test: DataFrame, spark: SparkSession): String = {

    train.createOrReplaceTempView("DB1")
    test.createOrReplaceTempView("DB2")

    val TrainingData = spark.sql("select Rid, features, FinalROLLBACK_REVERTED  from DB1")
    val TestingData = spark.sql("select Rid, features, FinalROLLBACK_REVERTED  from DB2")

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer().setInputCol("FinalROLLBACK_REVERTED").setOutputCol("indexedLabel").fit(TrainingData)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(TrainingData)

    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    // Train model. This also runs the indexers.
    val modelxx = pipeline.fit(TrainingData)

    // Make predictions.
    val predictions = modelxx.transform(TestingData)

    // Select example rows to display.
    // val finlaPrediction = predictions.select("Rid", "features", "FinalROLLBACK_REVERTED", "predictedLabel")

    // Case1 : BinaryClassificationEvaluator:----------------------------------------------------------
    val binaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol("indexedLabel").setRawPredictionCol("rawPrediction")

    var result1 = 0.0
    def printlnMetricCAse1(metricName: String): Double = {
      result1 = binaryClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      println(metricName + " = " + result1)

      result1
    }
    val ROC = printlnMetricCAse1("areaUnderROC")
    val PR = printlnMetricCAse1("areaUnderPR")

    // Case 2: MulticlassClassificationEvaluator:-----------------------------------------------------
    // Select (prediction, true label) and compute test error.
    val MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
    var result2 = 0.0
    def printlnMetricCase2(metricName: String): Double = {
      result2 = MulticlassClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      println(metricName + " = " + result2)
      result2
    }
    val accuracy = printlnMetricCase2("accuracy")
    val Precision = printlnMetricCase2("weightedPrecision")
    val Recall = printlnMetricCase2("weightedRecall")

    val finalResult = "ROC=" + ROC.toString() + "|" + "PR=" + PR.toString() + "|" + "accuracy=" + accuracy.toString() + "|" + "Precision=" + Precision.toString() + "|" + "Recall=" + Recall.toString()

    finalResult

  }

  // 3. Logistic Regrision
  def logisticRegrision(train: DataFrame, test: DataFrame, spark: SparkSession): String = {

    train.createOrReplaceTempView("DB1")
    test.createOrReplaceTempView("DB2")

    val TrainingData = spark.sql("select Rid, features, FinalROLLBACK_REVERTED as label from DB1") // for logistic regrision
    val TestingData = spark.sql("select Rid, features, FinalROLLBACK_REVERTED as label from DB2") // for logistic regrision

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(TrainingData)

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(TrainingData)

    // Train a DecisionTree model.
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setFamily("multinomial")

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))

    // Train model. This also runs the indexers.
    val modelxx = pipeline.fit(TrainingData)

    // Make predictions.
    val predictions = modelxx.transform(TestingData)

    // Select example rows to display.
    val finlaPrediction = predictions.select("Rid", "features", "label", "predictedLabel")

    predictions.show()

    // Case1 : BinaryClassificationEvaluator:----------------------------------------------------------
    val binaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol("indexedLabel").setRawPredictionCol("rawPrediction")
    var results1 = 0.0
    def printlnMetricCase1(metricName: String): Double = {

      results1 = binaryClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      println(metricName + " = " + results1)
      results1
    }
    val ROC = printlnMetricCase1("areaUnderROC")
    val PR = printlnMetricCase1("areaUnderPR")

    // Case 2: MulticlassClassificationEvaluator:-----------------------------------------------------
    // Select (prediction, true label) and compute test error.
    val MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
    var result2 = 0.0
    def printlnMetricCase2(metricName: String): Double = {

      result2 = MulticlassClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      println(metricName + " = " + result2)
      result2
    }
    val accuracy = printlnMetricCase2("accuracy")
    val Precision = printlnMetricCase2("weightedPrecision")
    val Recall = printlnMetricCase2("weightedRecall")

    val finalResult = "ROC=" + ROC.toString() + "|" + "PR=" + PR.toString() + "|" + "accuracy=" + accuracy.toString() + "|" + "Precision=" + Precision.toString() + "|" + "Recall=" + Recall.toString()

    finalResult

  }
  // 4.  Gradient Boosted Tree
  def gradientBoostedTree(train: DataFrame, test: DataFrame, spark: SparkSession): String = {

    train.createOrReplaceTempView("DB1")
    train.createOrReplaceTempView("DB2")

    val TrainingData = spark.sql("select Rid, features, FinalROLLBACK_REVERTED  from DB1").cache()
    val TestingData = spark.sql("select Rid, features, FinalROLLBACK_REVERTED  from DB2").cache()

    val labelIndexer = new StringIndexer().setInputCol("FinalROLLBACK_REVERTED").setOutputCol("indexedLabel").fit(TrainingData)

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(TrainingData)

    // Split the data into training and test sets (30% held out for testing).
    //    val Array(trainingData, testData) = Data.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    val gbt = new GBTClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures") // .setMaxIter(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

    // Train model. This also runs the indexers.
    val modelxx = pipeline.fit(TrainingData)

    // Make predictions.
    val predictions = modelxx.transform(TestingData)

    // Select example rows to display.

    // Case1 : BinaryClassificationEvaluator:----------------------------------------------------------

    var predictionsRDD = predictions.select("prediction", "FinalROLLBACK_REVERTED").rdd
    var predictionAndLabels = predictionsRDD.map { row => (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Double]) }

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    println("Area under ROC = " + metrics.areaUnderROC())
    println("Area under PR = " + metrics.areaUnderPR())

    val ROC = metrics.areaUnderROC()
    val PR = metrics.areaUnderPR()

    // Case 2: MulticlassClassificationEvaluator:-----------------------------------------------------
    // Select (prediction, true label) and compute test error.
    val MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")

    var result2 = 0.0
    def printlnMetric(metricName: String): Double = {

      result2 = MulticlassClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      println(metricName + " = " + result2)
      result2
    }
    val accuracy = printlnMetric("accuracy")
    val Precision = printlnMetric("weightedPrecision")
    val Recall = printlnMetric("weightedRecall")

    val finalResult = "ROC=" + ROC.toString() + "|" + "PR=" + PR.toString() + "|" + "accuracy=" + accuracy.toString() + "|" + "Precision=" + Precision.toString() + "|" + "Recall=" + Recall.toString()

    finalResult

  }

  // 5. Multilayer Perceptron Classifier
  def multilayerPerceptronClassifier(train: DataFrame, test: DataFrame, spark: SparkSession): String = {

    train.createOrReplaceTempView("DB1")
    train.createOrReplaceTempView("DB2")

    val TrainingData = spark.sql("select Rid, features, FinalROLLBACK_REVERTED as label from DB1")
    val TestingData = spark.sql("select Rid, features, FinalROLLBACK_REVERTED as label from DB2")

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(TrainingData)

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(TrainingData)

    val layers = Array[Int](100, 5, 4, 2)

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(100)

    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, trainer, labelConverter))

    // train the model
    val modelxx = pipeline.fit(TrainingData)

    // compute accuracy on the test set
    val predictions = modelxx.transform(TestingData)

    // predictions.show()

    // Case1 : BinaryClassificationEvaluator:----------------------------------------------------------
    var predictionsDF = predictions.select("prediction", "label")
    var predictionsRDD = predictions.select("prediction", "label").rdd
    var predictionAndLabels = predictionsRDD.map { row => (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Double]) }

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    println("Area under ROC = " + metrics.areaUnderROC())
    println("Area under PR = " + metrics.areaUnderPR())

    val ROC = metrics.areaUnderROC()
    val PR = metrics.areaUnderPR()

    // Case 2: MulticlassClassificationEvaluator:-----------------------------------------------------
    val accuracyevaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
    val weightedPrecisionevaluator = new MulticlassClassificationEvaluator().setMetricName("weightedPrecision")
    val weightedRecallevaluator = new MulticlassClassificationEvaluator().setMetricName("weightedRecall")

    println("Accuracy = " + accuracyevaluator.evaluate(predictionsDF))
    println("weightedPrecision = " + weightedPrecisionevaluator.evaluate(predictionsDF))
    println("weightedRecall = " + weightedRecallevaluator.evaluate(predictionsDF))

    val accuracy = accuracyevaluator.evaluate(predictionsDF)
    val Precision = weightedPrecisionevaluator.evaluate(predictionsDF)
    val Recall = weightedRecallevaluator.evaluate(predictionsDF)

    val finalResult = "ROC=" + ROC.toString() + "|" + "PR=" + PR.toString() + "|" + "accuracy=" + accuracy.toString() + "|" + "Precision=" + Precision.toString() + "|" + "Recall=" + Recall.toString()
    finalResult

  }

  def randomForestClassiferWithSplit(data: DataFrame): String = {

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)

    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))

    val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(10)

    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    val binaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction")
    var results1 = 0.0
    def printlnMetricCAse1(metricName: String): Double = {
      results1 = binaryClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      results1
    }
    val ROC = printlnMetricCAse1("areaUnderROC")
    val PR = printlnMetricCAse1("areaUnderPR")

    // Case 2: MulticlassClassificationEvaluator:OK -----------------------------------------------------
    // Select (prediction, true label) and compute test error.
    val MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
    var results2 = 0.0

    def printlnMetricCase2(metricName: String): Double = {
      results2 = MulticlassClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      results2
    }
    val accuracy = printlnMetricCase2("accuracy")
    val Precision = printlnMetricCase2("weightedPrecision")
    val Recall = printlnMetricCase2("weightedRecall")

    val result = "For RandomForest values are : " + ROC + " &&&&& " + PR + " &&&&& " + accuracy + " &&&&& " + Precision + " &&&&& " + Recall.toString()

    result
  }

  def decisionTreeClassifierWithSplit(data: DataFrame): String = {

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

    // Split the data into training and test sets (30% held out for testing).
    // val Array(trainingData, testData) = Data.randomSplit(Array(0.7, 0.3))
    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))

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

    // Case1 : BinaryClassificationEvaluator
    val binaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol("indexedLabel").setRawPredictionCol("rawPrediction")

    var result1 = 0.0
    def printlnMetricCAse1(metricName: String): Double = {
      result1 = binaryClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      println(metricName + " = " + result1)

      result1
    }
    val ROC = printlnMetricCAse1("areaUnderROC")
    val PR = printlnMetricCAse1("areaUnderPR")

    // Case 2: MulticlassClassificationEvaluator:
    // Select (prediction, true label) and compute test error.
    val MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
    var result2 = 0.0
    def printlnMetricCase2(metricName: String): Double = {
      result2 = MulticlassClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      println(metricName + " = " + result2)
      result2
    }
    val accuracy = printlnMetricCase2("accuracy")
    val Precision = printlnMetricCase2("weightedPrecision")
    val Recall = printlnMetricCase2("weightedRecall")

    val result = "For DecisionTreeClassifier values are : " + ROC + " &&&&& " + PR + " &&&&& " + accuracy + " &&&&& " + Precision + " &&&&& " + Recall.toString()

    result

  }

  def logisticRegrisionWithSplit(data: DataFrame): String = {

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)

    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))

    // Train a DecisionTree model.
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setFamily("multinomial")

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    val binaryClassificationEvaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction")
    var results1 = 0.0
    def printlnMetricCAse1(metricName: String): Double = {
      results1 = binaryClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      results1
    }
    val ROC = printlnMetricCAse1("areaUnderROC")
    val PR = printlnMetricCAse1("areaUnderPR")

    // Case 2: MulticlassClassificationEvaluator:-----------------------------------------------------
    // Select (prediction, true label) and compute test error.
    val MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
    var result2 = 0.0
    def printlnMetricCase2(metricName: String): Double = {

      result2 = MulticlassClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      println(metricName + " = " + result2)
      result2
    }
    val accuracy = printlnMetricCase2("accuracy")
    val Precision = printlnMetricCase2("weightedPrecision")
    val Recall = printlnMetricCase2("weightedRecall")

    val result = "For LogisticRegrision values are : " + ROC + " &&&&& " + PR + " &&&&& " + accuracy + " &&&&& " + Precision + " &&&&& " + Recall.toString()

    result
  }

  def gradientBoostedTreeWithSplit(data: DataFrame): String = {

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)

    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))

    val gbt = new GBTClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures") // .setMaxIter(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

    // Train model. This also runs the indexers.
    val modelxx = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = modelxx.transform(testData)

    // Case1 : BinaryClassificationEvaluator:----------------------------------------------------------

    var predictionsRDD = predictions.select("prediction", "label").rdd
    var predictionAndLabels = predictionsRDD.map { row => (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Double]) }

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    println("Area under ROC = " + metrics.areaUnderROC())
    println("Area under PR = " + metrics.areaUnderPR())

    val ROC = metrics.areaUnderROC()
    val PR = metrics.areaUnderPR()

    // Case 2: MulticlassClassificationEvaluator:-----------------------------------------------------
    // Select (prediction, true label) and compute test error.
    val MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")

    var result2 = 0.0
    def printlnMetric(metricName: String): Double = {

      result2 = MulticlassClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      println(metricName + " = " + result2)
      result2
    }
    val accuracy = printlnMetric("accuracy")
    val Precision = printlnMetric("weightedPrecision")
    val Recall = printlnMetric("weightedRecall")

    val result = "For GradientBoostedTree values are : " + ROC + " &&&&& " + PR + " &&&&& " + accuracy + " &&&&& " + Precision + " &&&&& " + Recall.toString()

    result
  }

  def multilayerPerceptronClassifierWithSplit(data: DataFrame): String = {

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))

    val layers = Array[Int](101, 5, 4, 2) // 97 instead of 100 -----

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(101) // 97 instead of 100 -----

    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, trainer, labelConverter))

    // train the model
    val modelxx = pipeline.fit(trainingData)

    // compute accuracy on the test set
    val predictions = modelxx.transform(testData)

    // predictions.show()

    // Case1 : BinaryClassificationEvaluator:----------------------------------------------------------
    var predictionsDF = predictions.select("prediction", "label")
    var predictionsRDD = predictions.select("prediction", "label").rdd
    var predictionAndLabels = predictionsRDD.map { row => (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Double]) }

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    println("Area under ROC = " + metrics.areaUnderROC())
    println("Area under PR = " + metrics.areaUnderPR())

    val ROC = metrics.areaUnderROC()
    val PR = metrics.areaUnderPR()

    // Case 2: MulticlassClassificationEvaluator:-----------------------------------------------------
    val accuracyevaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
    val weightedPrecisionevaluator = new MulticlassClassificationEvaluator().setMetricName("weightedPrecision")
    val weightedRecallevaluator = new MulticlassClassificationEvaluator().setMetricName("weightedRecall")

    println("Accuracy = " + accuracyevaluator.evaluate(predictionsDF))
    println("weightedPrecision = " + weightedPrecisionevaluator.evaluate(predictionsDF))
    println("weightedRecall = " + weightedRecallevaluator.evaluate(predictionsDF))

    val accuracy = accuracyevaluator.evaluate(predictionsDF)
    val Precision = weightedPrecisionevaluator.evaluate(predictionsDF)
    val Recall = weightedRecallevaluator.evaluate(predictionsDF)

    val result = "For MultilayerPerceptronClassifier values are : " + ROC + " &&&&& " + PR + " &&&&& " + accuracy + " &&&&& " + Precision + " &&&&& " + Recall.toString()

    result
  }
}
