package net.sansa_stack.ml.spark.classification.decisionTrees

import collection.JavaConverters._
import java.util.ArrayList
import java.util.HashSet
import java.util.Set
import net.sansa_stack.ml.spark.classification.decisionTrees.ConceptsGenerator._
// import net.sansa_stack.ml.spark.classification.decisionTrees.KB
import net.sansa_stack.ml.spark.classification.decisionTrees.DistTDTInducer.DistTDTInducer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLIndividual
import scala.util.Random

object ClassMembership {

  /**
    * Implementing class-membership
    */

  class ClassMembership (k: KB, sc: SparkSession) {

    val kb: KB = k
    val allExamples: RDD[OWLIndividual] = kb.getIndividuals
    val cg: ConceptsGenerator = new ConceptsGenerator(kb)
    val testConcepts: Array[OWLClassExpression] = cg.generateQueryConcepts(1, sc)
    val negTestConcepts: Array[OWLClassExpression] = Array.ofDim[OWLClassExpression](testConcepts.length)

    var c: Int = 0
    for (c <- testConcepts.indices)
      negTestConcepts(c) = kb.getDataFactory.getOWLObjectComplementOf(testConcepts(c))

    // Classification w.r.t. all query concepts
    val classification: RDD[((OWLClassExpression, OWLIndividual), Int)] =
                  kb.KB.getClassMembershipResult(testConcepts, negTestConcepts, allExamples)

    def bootstrap(noFolds: Int, spark: SparkSession): Unit = {
      
      println("\nBOOTSTRAP Experiment of " + noFolds + "folds")
      
      for (f <- 0 until noFolds) {
        println("\nFold #" + (f + 1))
        println(" *********************************")
        val split: Array[RDD[OWLIndividual]] = allExamples.randomSplit(Array(0.7, 0.3), 1L)
        val trainData: RDD[OWLIndividual] = split(0)
        val testData: RDD[OWLIndividual] = split(1)
  
        val cl : DistTDTInducer = new DistTDTInducer(kb, kb.concepts.count().toInt, spark)
  
        // training phase: using all examples but those in the f-th partition
        println("Training is starting...")
        
        val result = kb.getClassMembershipResults
        cl.training(result, trainData, testConcepts, negTestConcepts)
    
      } // fold Loop
    }

    
    
    //    def bootstrap(nFolds: Int, spark: SparkSession): Unit = {
//
//      println()
//      println(nFolds + "-fold BOOTSTRAP Experiment on ontology: ")
//      // val classifierClass: Class[_] = ClassLoader.getSystemClassLoader.loadClass(className)
//      val nOfConcepts: Int = if (testConcepts != null) testConcepts.length else 1
//
//      val Generator: Random = new Random()
//      val noTestExs: Array[Int] = Array.ofDim[Int](nFolds)
//
//      // main loop on the folds
//      for (f <- 0 until nFolds) {
//        println("\n\nFold #" + (f + 1))
//        println(" ******************************************************************")
//
//        val trainingExsSet: Set[Integer] = new HashSet[Integer]()
//        val trainRDD = spark.sparkContext.parallelize(trainingExsSet.asScala.toSeq)
//
//        val testingExsSet: Set[Integer] = new HashSet[Integer]()
//        val testRDD = spark.sparkContext.parallelize(testingExsSet.asScala.toSeq)
//
//        val rand1 = new ArrayList[Integer]
//        for (r <- 0 until allExamples.count.toInt)
//          rand1.add(Generator.nextInt(allExamples.count.toInt))
//
//        val newRDD = spark.sparkContext.parallelize(rand1.asScala)
//        trainRDD.union(newRDD)
//        // trainingExsSet.add(Generator.nextInt(allExamples.count.toInt))
//
//        val r = 0 to allExamples.count.toInt
//        val rand2 = spark.sparkContext.parallelize(r)
//
//        if (!trainRDD.collect().contains(rand2)) {
//          testRDD.union(rand2.asInstanceOf[RDD[Integer]])
//        }
//        /* for (r <- 0 until allExamples.count.toInt) {
//          if (!trainRDD.collect().contains(r))
//             testRDD.union(r)
//        } */
//
//
//        /* var trainingExs: Array[Integer] = Array.ofDim[Integer](0)
//        var testExs: Array[Integer] = Array.ofDim[Integer](0)
//
//        trainingExs = trainingExsSet.toArray(trainingExs)
//        testExs = testingExsSet.toArray(testExs) */
//
//        val cl : TDTInducer = new TDTInducer(kb, kb.concepts.count().toInt, spark)
//
//        /* val cl: TDTInducer = (classifierClass.getConstructor(classOf[KB], classOf[Int]))
//        .newInstance(kb, nOfConcepts).asInstanceOf[TDTInducer] */
//
//        noTestExs(f) = testRDD.count.toInt
//
//        // training phase: using all examples but those in the f-th partition
//        println("Training is starting...")
//
//
//
//    //    val results: Array[Array[Int]] = kb.getClassMembershipResult
// //         cl.training(results, trainingExs, testConcepts, negTestConcepts)
//       // cl.training(results, trainRDD, testConcepts, negTestConcepts)
//
//
//        // val labels: Array[Array[Int]] = cl.test(f, testRDD, testConcepts)
//
//      }// for loop
//
//    }  // bootstrap function

  } // class
}