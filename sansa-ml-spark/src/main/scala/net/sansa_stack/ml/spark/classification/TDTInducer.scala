package net.sansa_stack.ml.spark.classification

import java.io.PrintStream
import java.util.ArrayList
import java.util.List
import java.util.Arrays
import java.util.HashSet
import collection.JavaConverters._
import scala.collection

import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLIndividual
import org.semanticweb.HermiT.Reasoner

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import net.sansa_stack.ml.spark.classification._
import net.sansa_stack.ml.spark.classification.KB.KB
import net.sansa_stack.ml.spark.classification.TDTClassifier.TDTClassifier

/*
 * Class for the induction of Terminological Decision Tree
 */

object TDTInducer {
    var stream: PrintStream = _
    
    val sparkSession = SparkSession.builder
			.master("local[*]")
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.appName("Terminological Decision Tree Inducer")
			.getOrCreate()

			val conf = new SparkConf().setAppName("TDTInducer").setMaster("local[*]")  // local mode
			val sc = new SparkContext(conf)


class TDTInducer(var kb: KB, var nConcepts: Int) {

//for each query concept induce an ensemble
  var trees: Array[DLTree] = new Array[DLTree](nConcepts)

  var cl: TDTClassifier = new TDTClassifier(kb)

  
  /*
   * Function for training 
   */
  def training(results: Array[Array[Int]], trainingExs: Array[Integer],
                        testConcepts: Array[OWLClassExpression],
                        negTestConcepts: Array[OWLClassExpression]): Unit = {
  
    val op: RefinementOperator = new RefinementOperator(kb)
    val reasoner: Reasoner = kb.getReasoner
    val allExamples: RDD[OWLIndividual] = kb.getIndividuals

    val trainingExsSet: HashSet[Integer] = new HashSet[Integer](Arrays.asList(trainingExs: _*))

    val length: Int = if (testConcepts != null) testConcepts.length else 1
    
    for (c <- 0 until length) {
      
      val posExs: ArrayList[Integer] = new ArrayList[Integer]()
      val pos = sc.parallelize(posExs.asScala)
      
      val negExs: ArrayList[Integer] = new ArrayList[Integer]()
      val neg = sc.parallelize(negExs.asScala)
      
      val undExs: ArrayList[Integer] = new ArrayList[Integer]()
      val und = sc.parallelize(undExs.asScala)
      
      println("--- Query Concept #%d \n", c)
      
      splitting(trainingExs, results, c, pos, neg, und)
      
      var prPos: Double = pos.count.toDouble / (trainingExs.length)
      var prNeg: Double = neg.count.toDouble / (trainingExs.length)
      println("Training set composition: " + pos.count() + " - " + neg.count() + "-" + und.count())
      
      val normSum: Double = prPos + prNeg
      if (normSum == 0) {
        prPos = .5
        prNeg = .5
      } else {
        prPos = prPos / normSum
        prNeg = prNeg / normSum
      }
      println("New learning problem prepared.\n", c)
      println("Learning a tree ")
      trees(c) = cl.induceDLTree(kb.getDataFactory.getOWLThing, pos, neg, und, 50, prPos, prNeg)
   
    }

// These instances should be divided into negative instances, positive and uncertain 

  }

  /*
   * Function for splitting the training examples into positive, negative and undefined examples
   */
  
  def splitting(trainingExs: Array[Integer], classifications: Array[Array[Int]], c: Int,
                posExs: RDD[Integer],
                negExs: RDD[Integer],
                undExs: RDD[Integer]): Unit = {
    
    var BINARYCLASSIFICATION : Boolean = false
    val T : List[Integer]= new ArrayList[Integer]
    
    for (e <- 0 until trainingExs.length) {
 
      T.add(trainingExs(e))
      val Train = sc.parallelize(T.asScala)
      
      if (classifications(c)(trainingExs(e)) == +1) posExs.union(Train)
      else if (!BINARYCLASSIFICATION) {
          if (classifications(c)(trainingExs(e)) == -1)
            negExs.union(Train)
          else undExs.union(Train)
      } else negExs.union(Train)
    }
  }

 def test( f: Int, testExs: Array[Integer], testConcepts: Array[OWLClassExpression]): Array[Array[Int]] = {

   // classifier answers for each example and for each concept
    val labels: Array[Array[Int]] = Array.ofDim[Int](testExs.length, nConcepts)
    for (te <- 0 until testExs.length) {
      val indTestEx: Int = testExs(te)
      println("\n\nFold #" + f)
      println(" --- Classifying Example " + (te + 1) + "/" + testExs.length + " [" + indTestEx + "] " +
          kb.getIndividuals().map{indTestEx => indTestEx})
      
      val indClassifications: Array[Int] = Array.ofDim[Int](nConcepts)

      labels(te) = Array.ofDim[Int](nConcepts)
      val L = testExs(indTestEx)
      
      for (i <- 0 until nConcepts - 1) {
        labels(te)(i) = cl.classify(kb.getIndividuals().map{L => L}.asInstanceOf[OWLIndividual], trees(i))
      }
    }
    labels
  }

  def getComplexityValues(): Array[Double] = {

    // a measure to express the model complexity (e.g. the number of nodes in a tree)
    val complexityValue: Array[Double] = Array.ofDim[Double](trees.length)
    for (i <- 0 until trees.length) {
      val current: Double = trees(i).getComplexityMeasure
      complexityValue(i) = current
    }
    complexityValue
  }

}
}