package net.sansa_stack.ml.spark.classification.decisionTrees

import net.sansa_stack.ml.spark.classification.decisionTrees.DLTree.DLTree
import net.sansa_stack.ml.spark.classification.decisionTrees.TDTClassifier.TDTClassifier2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.semanticweb.owlapi.model._


/**
 * Class for the induction of Distributed Terminological Decision Tree
 * @author Heba Mohamed
 */

object DistTDTInducer {
  
  /**
   * @param kb The knowledgebase
   * @param nConcepts Number of the query concepts
   * @param sc The Apache Spark context
   */
 
  class DistTDTInducer(var kb: KB, var nConcepts: Int, var sc: SparkSession) extends Serializable{

    var trees: Array[DLTree] = new Array[DLTree](nConcepts)

    /**
     *  Function for training the algorithm
     *
     *  @param results The classification results
     *  @param trainingExs RDD of the training individuals
     *  @param testConcepts Testing query concepts
     *  @param negTestConcepts Negative testing query concepts
     */
    def training(results: RDD[((OWLClassExpression, OWLIndividual), Int)],
                 trainingExs: RDD[OWLIndividual],
                 testConcepts: Array[OWLClassExpression],
                 negTestConcepts: Array[OWLClassExpression]): Unit = {
  
      val cl: TDTClassifier2 = new TDTClassifier2(kb) with Serializable

      val length: Int = if (testConcepts != null) testConcepts.length else 1

      for (c <- 0 until length) {

        println("\n--- Query Concept # " + (c + 1))

        // These instances should be divided into negative, positive and uncertain instances
        // split._1 = posExs,    split._2 = negExs,  split._3 = undExs
        val split = splitting(trainingExs, results, testConcepts(c))

        var prPos = split._1.count / trainingExs.count.toDouble
        var prNeg = split._2.count / trainingExs.count.toDouble
        
        println("Training set composition: " + split._1.count() + " - " + split._2.count() + " - " + split._3.count())

        val total: Double = prPos + prNeg
        if (total == 0) {
          prPos = 0.5
          prNeg = 0.5
        } else {
          prPos = prPos / total
          prNeg = prNeg / total
        }

        println("\nNew learning problem prepared " + (c + 1))
        trees(c) = cl.induceDLTree(kb.getDataFactory.getOWLThing, split._1, split._2, split._3, 10, prPos, prNeg)
        println("\n--- Tree " + (c + 1) + " was induced. \n")
        
//        trainingExs.unpersist()

      }
    }

    /*
     * Function for splitting the training examples into positive, negative and undefined examples
     */

//    def splitting(trainingExs: RDD[OWLIndividual],
//                  classifications: RDD[((OWLClassExpression, OWLIndividual), Int)],
//                  c: OWLClassExpression): (RDD[String], RDD[String], RDD[String]) = {
//
//      pos = classifications.filter(_._1._1 == c).filter(_._2 == +1).map(_._2.toString()).cache()
//      neg = classifications.filter(_._1._1 == c).filter(_._2 == -1).map(_._2.toString()).cache()
//      und = classifications.filter(_._1._1 == c).filter(_._2 == 0).map(_._2.toString())
//      (pos, neg, und)
//    }
    /**
     *  Function for splitting the individuals into positive, negative and undefined individuals
     *
     *  @param trainingExs RDD of the training individuals
     *  @param classifications The classification results
     *  @param c The testing OWLClassExpression
     *  @return (RDD[positive], RDD[negative], RDD[undefined])
     */
  
    def splitting(trainingExs: RDD[OWLIndividual],
                  classifications: RDD[((OWLClassExpression, OWLIndividual), Int)],
                  c: OWLClassExpression): (RDD[OWLIndividual], RDD[OWLIndividual], RDD[OWLIndividual]) = {
  
      classifications.persist(StorageLevel.MEMORY_AND_DISK)
      
//      val train = trainingExs.map(t => (c, t)).collect.toMap
//      val trainBC = sc.sparkContext.broadcast(train)
//
//      val pos = classifications.filter(cl => cl._2 == 1 && trainBC.value.contains(cl._1._1))
//                               .map(_._1._2)
//
//      val neg = classifications.filter(cl => cl._2 == -1 && trainBC.value.contains(cl._1._1))
//                               .map(_._1._2)
//
//      val und = classifications.filter(cl => cl._2 == 0 && trainBC.value.contains(cl._1._1))
//                               .map(_._1._2)
      
      val tr = trainingExs.map(t => ((c, t), 1))
      val pos = classifications.filter(_._2 == 1).join(tr).map(_._1._2)
      val neg = classifications.filter(_._2 == -1).join(tr).map(_._1._2)
      val und = classifications.filter(_._2 == 0).join(tr).map(_._1._2)
      
      classifications.unpersist()
  
      (pos, neg, und)
    }
  
    /**
     *  Function for testing the algorithm
     *
     *  @param f The current fold number
     *  @param testExs RDD of testing individuals
     *  @param testConcepts Array of testing query concepts
     *  @return RDD[((OWLClassExpression, OWLIndividual), Int)]
     */
  
    def test (f: Int,
              testExs: RDD[OWLIndividual],
              testConcepts: Array[OWLClassExpression]): RDD[((OWLClassExpression, OWLIndividual), Int)] = {
  
      val cl: TDTClassifier2 = new TDTClassifier2(kb) with Serializable
      //      testExs.persist(StorageLevel.MEMORY_AND_DISK)
      val count = testExs.count().toInt
  
      // for each query concept induce an example
      var labels = testExs.map(t => ((testConcepts(0), t), 0))
      
      // classifier answers for each example and for each concept
      for (c <- testConcepts.indices) {
        println(" Testing Examples " + count)
        labels = testExs.map { indv =>
          val l = cl.classify(indv, trees(c))
//          println("result = " + l)
          ((testConcepts(c), indv), l)
        }
      }
//      testExs.unpersist()
      labels
    }
  
  }
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
  
  
   
//    def test (f: Int, testExs: RDD[OWLIndividual], testConcepts: Array[OWLClassExpression]): Array[Array[Int]] = {
//
//      // classifier answers for each example and for each concept
//      val labels: Array[Array[Int]] = Array.ofDim[Int](testExs.count.toInt, nConcepts)
//
//      for (t <- 0 until testExs.count.toInt) {
//        val indTestEx = testExs.take(t + 1).apply(t)
//        println("\n\nFold #" + (f + 1))
//        println(" ---\n Classifying Example " + (t + 1) + "/" + testExs.count.toInt + ": " + indTestEx)
//
//        // labels(t) = Array.ofDim[Int](nConcepts)
//
//
// //        for (i <- 0 until nConcepts - 1) {
// //          labels(t)(i) = cl.classify(indTestEx, trees(i))
// //        }
//      }
//      labels
//    }
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

