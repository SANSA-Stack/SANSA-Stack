package net.sansa_stack.ml.spark.classification.decisionTrees

import collection.JavaConverters._
import java.util.ArrayList
import java.util.HashSet
import java.util.Set
import net.sansa_stack.ml.spark.classification.decisionTrees.ConceptsGenerator._
import net.sansa_stack.ml.spark.classification.decisionTrees.KB
import net.sansa_stack.ml.spark.classification.decisionTrees.TDTInducer.TDTInducer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLIndividual
import scala.util.Random

object ClassMembership {

  protected var kb: KB = _
  protected var allExamples: RDD[OWLIndividual] = _
  protected var testConcepts: Array[OWLClassExpression] = _
  protected var negTestConcepts: Array[OWLClassExpression] = _
//  protected var classification: Array[Array[Int]] = _
  protected var classification: RDD[((OWLClassExpression, OWLIndividual), Int)] = _


  /**
    * Implementing class-membership
    */

  class ClassMembership (k: KB, sc: SparkSession) {

    kb = k
    allExamples = kb.getIndividuals
    val cg: ConceptsGenerator = new ConceptsGenerator(kb)
    testConcepts = cg.generateQueryConcepts(1, sc)
    negTestConcepts = Array.ofDim[OWLClassExpression](testConcepts.length)

    var c: Int = 0
    for (c <- 0 until testConcepts.length)
      negTestConcepts(c) = kb.getDataFactory.getOWLObjectComplementOf(testConcepts(c))

    // Classification w.r.t. all query concepts
    classification = kb.KB.getClassMembershipResult(testConcepts, negTestConcepts, allExamples)


    def bootstrap(nFolds: Int, className: String, spark: SparkSession): Unit = {

      println()
      println(nFolds + "-fold BOOTSTRAP Experiment on ontology: ")
      // val classifierClass: Class[_] = ClassLoader.getSystemClassLoader.loadClass(className)
      val nOfConcepts: Int = if (testConcepts != null) testConcepts.size else 1

      var Generator: Random = new Random()
      val ntestExs: Array[Int] = Array.ofDim[Int](nFolds)

      // main loop on the folds
      for (f <- 0 until nFolds) {
        println("\n\nFold #" + (f + 1))
        println(" ******************************************************************")

        val trainingExsSet: Set[Integer] = new HashSet[Integer]()
        val trainRDD = spark.sparkContext.parallelize(trainingExsSet.asScala.toSeq)

        val testingExsSet: Set[Integer] = new HashSet[Integer]()
        val testRDD = spark.sparkContext.parallelize(testingExsSet.asScala.toSeq)

        val rand1 = new ArrayList[Integer]
        for (r <- 0 until allExamples.count.toInt)
          rand1.add(Generator.nextInt(allExamples.count.toInt))

        val newRDD = spark.sparkContext.parallelize(rand1.asScala)
        trainRDD.union(newRDD)
        // trainingExsSet.add(Generator.nextInt(allExamples.count.toInt))

        val r = 0 to allExamples.count.toInt
        val rand2 = spark.sparkContext.parallelize(r)

        if (!trainRDD.collect().contains(rand2)) {
          testRDD.union(rand2.asInstanceOf[RDD[Integer]])
        }
        /* for (r <- 0 until allExamples.count.toInt) {
          if (!trainRDD.collect().contains(r))
             testRDD.union(r)
        } */


        /* var trainingExs: Array[Integer] = Array.ofDim[Integer](0)
        var testExs: Array[Integer] = Array.ofDim[Integer](0)

        trainingExs = trainingExsSet.toArray(trainingExs)
        testExs = testingExsSet.toArray(testExs) */

        val cl : TDTInducer = new TDTInducer(kb, kb.concepts.count().toInt, spark)

        /* val cl: TDTInducer = (classifierClass.getConstructor(classOf[KB], classOf[Int]))
        .newInstance(kb, nOfConcepts).asInstanceOf[TDTInducer] */

        ntestExs(f) = testRDD.count.toInt

        // training phase: using all examples but those in the f-th partition
        println("Training is starting...")



//        val results: Array[Array[Int]] = kb.getClassMembershipResult
//         cl.training(results, trainingExs, testConcepts, negTestConcepts)
       // cl.training(results, trainRDD, testConcepts, negTestConcepts)


        // val labels: Array[Array[Int]] = cl.test(f, testRDD, testConcepts)

      }// for loop

    }  // bootstrap function

  } // class
}