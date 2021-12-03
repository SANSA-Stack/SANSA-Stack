package net.sansa_stack.ml.spark.classification.decisionTrees

import collection.JavaConverters._
import java.io.PrintStream
import java.util.ArrayList
import java.util.List
import net.sansa_stack.ml.spark.classification._
import net.sansa_stack.ml.spark.classification.decisionTrees.KB
import net.sansa_stack.ml.spark.classification.decisionTrees.TDTClassifiers.TDTClassifiers
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.model._
import org.semanticweb.HermiT.Reasoner


/*
 * Class for the induction of Terminological Decision Tree
 */

object TDTInducer {
  // var stream: PrintStream = _

  class TDTInducer(var kb: KB, var nConcepts: Int, var sc: SparkSession) extends Serializable{

    // for each query concept induce an ensemble
    var trees: Array[DLTree] = new Array[DLTree](nConcepts)

    var cl: TDTClassifiers = new TDTClassifiers(kb, sc)
    var pos: RDD[String] = _
    var neg: RDD[String] = _
    var und: RDD[String] = _


    /*
     * Function for training the algorithm
     */
    def training(results: RDD[(OWLClassExpression, OWLIndividual, Int)], trainingExs: RDD[OWLIndividual],
                 testConcepts: Array[OWLClassExpression],
                 negTestConcepts: Array[OWLClassExpression]): Unit = {

//      val op: RefinementOperator = new RefinementOperator(kb)
//      val reasoner: Reasoner = kb.getReasoner
//      val allExamples: RDD[OWLIndividual] = kb.getIndividuals

      // val trainingExsSet: HashSet[Integer] = new HashSet[Integer](Arrays.asList(trainingExs: _*))

      val length: Int = if (testConcepts != null) testConcepts.size else 1

      for (c <- 0 until length) {

        println("\n--- Query Concept # " + (c + 1))

        // These instances should be divided into negative instances, positive and uncertain
        // split._1 = posExs,    split._2 = negExs,  split._3 = undExs
        val split = splitting(trainingExs, results, testConcepts(c))

        var prPos: Double = split._1.count.toDouble / trainingExs.count.toInt
        var prNeg: Double = split._2.count.toDouble / trainingExs.count.toInt
        println("Training set composition: " + split._1.count() + " - " + split._2.count() + " - " + split._3.count())

        val Sum: Double = prPos + prNeg
        if (Sum == 0) {
          prPos = 0.5
          prNeg = 0.5
        } else {
          prPos = prPos / Sum
          prNeg = prNeg / Sum
        }
        println("\nNew learning problem prepared " + (c + 1))
        println("Learning a tree ")
        trees(c) = cl.induceDLTree(kb.getDataFactory.getOWLThing, split._1, split._2, split._3, 50, prPos, prNeg)

      }
    }

    /*
     * Function for splitting the training examples into positive, negative and undefined examples
     */

    def splitting(trainingExs: RDD[OWLIndividual],
                  classifications: RDD[(OWLClassExpression, OWLIndividual, Int)],
                  c: OWLClassExpression): (RDD[String], RDD[String], RDD[String]) = {

      pos = classifications.filter(_._1 == c).filter(_._3 == +1).map(_._2.toString()).cache()
      neg = classifications.filter(_._1 == c).filter(_._3 == -1).map(_._2.toString()).cache()
      und = classifications.filter(_._1 == c).filter(_._3 == 0).map(_._2.toString())
      (pos, neg, und)
    }

    //    var BINARYCLASSIFICATION : Boolean = false

    // val ppos = classifications.filter(x => x._2 == +1).cache()
    // ppos.take(50).foreach(println(_))

    //    if (pos.count.toInt == 0) {
    //      if (!BINARYCLASSIFICATION) {
    //        neg = classifications.filter(_._2 == -1).map(_._1._2.toString()).cache()
    //      }
    //    }
    //    else{
    //      und = classifications.filter(_._2 == 0).map(_._1._2.toString())
    //    }

    //    var pos = new ArrayList[String]()
    //    var neg = new ArrayList[String]()
    //    var und = new ArrayList[String]()
    //    for (i <-0 until trainingExs.count.toInt) {

    //      val trainValue = trainingExs.take(i+1).apply(i)
    // var trainIndex = TExs.lookup(trainValue)
    // println("\nvalue : " + trainValue)

    //      val trainIndex = trainingExs.take(trainingExs.count.toInt).indexOf(trainValue)
    // println("index : " + trainIndex)




    //      if (trainIndex != -1) {
    //        val value = trainValue.toString()
    //        if (classifications(c)(trainIndex) == +1)
    //            pos.add(value)
    //        else if (!BINARYCLASSIFICATION) {
    //          if (classifications(c)(trainIndex) == -1)
    //            neg.add(value)
    //        else
    //            und.add(value)
    //      }
    //      else
    //          neg.add(value)
    //     }
    //   }
    //    var posExs = sc.sparkContext.parallelize(pos.asScala)
    //    var negExs = sc.sparkContext.parallelize(neg.asScala)
    //    var undExs = sc.sparkContext.parallelize(und.asScala)


    /*
   * Function for testing the algorithm
   */
    def test (f: Int, testExs: RDD[OWLIndividual], testConcepts: Array[OWLClassExpression]): Array[Array[Int]] = {

      // classifier answers for each example and for each concept
      val labels: Array[Array[Int]] = Array.ofDim[Int](testExs.count.toInt, nConcepts)

      for (t <- 0 until testExs.count.toInt) {
        val indTestEx = testExs.take(t + 1).apply(t)
        println("\n\nFold #" + (f + 1))
        println(" ---\n Classifying Example " + (t + 1) + "/" + testExs.count.toInt + " [" + indTestEx + "] ")

        // labels(t) = Array.ofDim[Int](nConcepts)


        for (i <- 0 until nConcepts - 1) {
          labels(t)(i) = cl.classify(indTestEx, trees(i))
        }
      }
      labels
    }
    //    val TList : List[Integer]= new ArrayList[Integer]
    //    var T = sc.sparkContext.parallelize(TList.asScala)
    //
    //    var TExs = trainingExs.zipWithIndex()
    //    for (e <- 0 until trainingExs.count.toInt) {
    //
    //     var index = TExs.lookup(e)
    //     T.union(index)
    // val Train = sc.sparkContext.parallelize(T.asScala)

    /* if (classifications(c)(TExs.lookup(e)) == +1) posExs.union(T)
    else if (!BINARYCLASSIFICATION) {
        if (classifications(c)(TExs.lookup(e)) == -1)
          negExs.union(T)
        else undExs.union(T)
    } else negExs.union(T) */
    // }



    /* def getComplexityValues(sc: SparkSession): Array[Double] = {
       // a measure to express the model complexity (e.g. the number of nodes in a tree)
       val complexityValue: Array[Double] = Array.ofDim[Double](trees.length)
       for (i <- 0 until trees.length) {
         val current: Double = trees(i).getComplexityMeasure(sc)
         complexityValue(i) = current
       }
       complexityValue
     } */

  }
}
