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
import org.semanticweb.owlapi.model.OWLNamedIndividual
import org.semanticweb.HermiT.Reasoner

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import net.sansa_stack.ml.spark.classification._
import net.sansa_stack.ml.spark.classification.KB.KB
import net.sansa_stack.ml.spark.classification.TDTClassifiers.TDTClassifiers

/*
 * Class for the induction of Terminological Decision Tree
 */

object TDTInducer {
    var stream: PrintStream = _
    
class TDTInducer(var kb: KB, var nConcepts: Int, var sc: SparkSession) {

//for each query concept induce an ensemble
  var trees: Array[DLTree] = new Array[DLTree](nConcepts)

  var cl: TDTClassifiers = new TDTClassifiers(kb, sc)

  
  /*
   * Function for training 
   */
  def training(results: Array[Array[Int]], trainingExs: RDD[Integer],
                        testConcepts: Array[OWLClassExpression],
                        negTestConcepts: Array[OWLClassExpression]): Unit = {
  
    val op: RefinementOperator = new RefinementOperator(kb)
    val reasoner: Reasoner = kb.getReasoner
    val allExamples: RDD[OWLIndividual] = kb.getIndividuals

    //val trainingExsSet: HashSet[Integer] = new HashSet[Integer](Arrays.asList(trainingExs: _*))

    val length: Int = if (testConcepts != null) testConcepts.size else 1
    
    for (c <- 0 until length) {
      
      val posExs: ArrayList[String] = new ArrayList[String]()
      val pos = sc.sparkContext.parallelize(posExs.asScala)
      
      val negExs: ArrayList[String] = new ArrayList[String]()
      val neg = sc.sparkContext.parallelize(negExs.asScala)
      
      val undExs: ArrayList[String] = new ArrayList[String]()
      val und = sc.sparkContext.parallelize(undExs.asScala)
      
      println("--- Query Concept #%d \n", c)
      
      // These instances should be divided into negative instances, positive and uncertain 
      splitting(trainingExs, results, c, pos, neg, und)
      
      var prPos: Double = pos.count.toDouble / (trainingExs.count.toInt)
      var prNeg: Double = neg.count.toDouble / (trainingExs.count.toInt)
      println("Training set composition: " + pos.count() + " - " + neg.count() + "-" + und.count())
      
      val normSum: Double = prPos + prNeg
      if (normSum == 0) {
        prPos = 0.5
        prNeg = 0.5
      } else {
        prPos = prPos / normSum
        prNeg = prNeg / normSum
      }
      println("New learning problem prepared.\n", c)
      println("Learning a tree ")
      trees(c) = cl.induceDLTree(kb.getDataFactory.getOWLThing, pos, neg, und, 50, prPos, prNeg)

    }
  }

  /*
   * Function for splitting the training examples into positive, negative and undefined examples
   */
  
  def splitting(trainingExs: RDD[Integer], classifications: Array[Array[Int]], c: Int,
                posExs: RDD[String],
                negExs: RDD[String],
                undExs: RDD[String]): Unit = {
    
    var BINARYCLASSIFICATION : Boolean = false
    val TList : List[Integer]= new ArrayList[Integer]
    var T = sc.sparkContext.parallelize(TList.asScala)
    
    var TExs = trainingExs.zipWithIndex()
    for (e <- 0 until trainingExs.count.toInt) {
      
     var index = TExs.lookup(e)
      //T.union(index)
      //val Train = sc.sparkContext.parallelize(T.asScala)
      
      /*if (classifications(c)(TExs.lookup(e)) == +1) posExs.union(T)
      else if (!BINARYCLASSIFICATION) {
          if (classifications(c)(TExs.lookup(e)) == -1)
            negExs.union(T)
          else undExs.union(T)
      } else negExs.union(T)*/
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

 /* def getComplexityValues(sc: SparkSession): Array[Double] = {

    // a measure to express the model complexity (e.g. the number of nodes in a tree)
    val complexityValue: Array[Double] = Array.ofDim[Double](trees.length)
    for (i <- 0 until trees.length) {
      val current: Double = trees(i).getComplexityMeasure(sc)
      complexityValue(i) = current
    }
    complexityValue
  }*/

}
}