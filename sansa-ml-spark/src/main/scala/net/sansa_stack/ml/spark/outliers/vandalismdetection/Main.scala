package net.sansa_stack.ml.spark.outliers.vandalismdetection

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.{ SparkContext, RangePartitioner }

object Main {

  def main(args: Array[String]) {

    val Start = new VandalismDetection()
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("VandalismDetector")
    val sc = new SparkContext(sparkConf)

    println("*********************************************************************")
    println("Choose (1) for  Distributed RDF Parser Model or (2) for Distributed XML parser && Vandalism Detectioin ")
    val num = scala.io.StdIn.readLine()
    // Distributed RDF Parser:
    if (num == "1") {

      Start.Start_RDF_Parser_Appraoch(sc)
    } // Distributed Standard Parser and Vandalism Detection : 
    else if (num == "2") {

      val Training_Data = Start.Training_Start_StandardXMLParser_VD(sc)
      val Testing_Data = Start.Testing_Start_StandardXMLParser_VD(sc)

      val OBJClassifiers = new Classifiers()

      //1.Random Forest Classifer:
      val RandomForestClassifer_Values = OBJClassifiers.RandomForestClassifer(Training_Data, Testing_Data, sc)

      //2.DecisionTreeClassifier
      val DecisionTreeClassifier_values = OBJClassifiers.DecisionTreeClassifier(Training_Data, Testing_Data, sc)

      // 3.LogisticRegrision
      val LogisticRegrision_values = OBJClassifiers.LogisticRegrision(Training_Data, Testing_Data, sc)

      //4.GradientBoostedTree
      val GradientBoostedTree_values = OBJClassifiers.GradientBoostedTree(Training_Data, Testing_Data, sc)

      //5.MultilayerPerceptronClassifier
      val MultilayerPerceptronClassifier_values = OBJClassifiers.MultilayerPerceptronClassifier(Training_Data, Testing_Data, sc)

      
      println(RandomForestClassifer_Values)
      println(DecisionTreeClassifier_values)
      println(LogisticRegrision_values)
      println(GradientBoostedTree_values)
      println(MultilayerPerceptronClassifier_values)

    }

  }
}