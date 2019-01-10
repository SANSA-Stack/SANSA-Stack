package net.sansa_stack.ml.spark.outliers.vandalismdetection

import org.apache.spark.{ RangePartitioner, SparkConf, SparkContext }
import org.apache.spark.sql._

object Main {

  def main(args: Array[String]) {

    val vd = new VandalismDetection()
    val spark = SparkSession.builder().master("local[*]").appName("VandalismDetector").getOrCreate()

    println("*********************************************************************")
    println("Choose (1) for  Distributed RDF Parser Model or (2) for Distributed XML parser && Vandalism Detectioin ")
    val rdfType = args(0)
    val input = args(1)
    val prefixes = args(2)
    val metaFile = args(3)
    val truthFile = args(4)
    // Distributed RDF Parser:
    if (rdfType == "1") {

      vd.parseRDF(spark, input, prefixes, rdfType)
    } // Distributed Standard Parser and Vandalism Detection:
    else if (rdfType == "2") {

      val Training_Data = vd.parseStandardXML(input, metaFile, truthFile, spark)
      val Testing_Data = vd.parseStandardXML(input, metaFile, truthFile, spark)

      // 1.Random Forest Classifer:
      val RandomForestClassifer_Values = Classifier.randomForestClassifer(Training_Data, Testing_Data, spark)

      // 2.DecisionTreeClassifier
      val DecisionTreeClassifier_values = Classifier.decisionTreeClassifier(Training_Data, Testing_Data, spark)

      // 3.LogisticRegrision
      val LogisticRegrision_values = Classifier.logisticRegrision(Training_Data, Testing_Data, spark)

      // 4.GradientBoostedTree
      val GradientBoostedTree_values = Classifier.gradientBoostedTree(Training_Data, Testing_Data, spark)

      // 5.MultilayerPerceptronClassifier
      val MultilayerPerceptronClassifier_values = Classifier.multilayerPerceptronClassifier(Training_Data, Testing_Data, spark)

      println(RandomForestClassifer_Values)
      println(DecisionTreeClassifier_values)
      println(LogisticRegrision_values)
      println(GradientBoostedTree_values)
      println(MultilayerPerceptronClassifier_values)
    }

  }
}
