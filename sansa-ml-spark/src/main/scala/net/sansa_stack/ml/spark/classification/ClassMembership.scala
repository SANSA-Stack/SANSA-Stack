package net.sansa_stack.ml.spark.classification

import java.io.PrintStream
import java.util.{ ArrayList, HashSet, Set }

import scala.collection
import scala.util.Random

import collection.JavaConverters._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.model.{ OWLClassExpression, OWLIndividual, OWLNamedIndividual }

import net.sansa_stack.ml.spark.classification
import net.sansa_stack.ml.spark.classification.ConceptsGenerator._
import net.sansa_stack.ml.spark.classification.KB.KB
import net.sansa_stack.ml.spark.classification.TDTInducer.TDTInducer

object ClassMembership {

  protected var kb: KB = _
  protected var allExamples: RDD[OWLIndividual] = _
  protected var testConcepts: Array[OWLClassExpression] = _
  protected var negTestConcepts: Array[OWLClassExpression] = _
  protected var classification: Array[Array[Int]] = _

  /**
   * Implementing class-membership
   */
  class ClassMembership(k: KB, sc: SparkSession) {

    kb = k
    allExamples = kb.getIndividuals
    val cg: ConceptsGenerator = new ConceptsGenerator(kb)
    testConcepts = cg.generateQueryConcepts(1, sc)
    negTestConcepts = Array.ofDim[OWLClassExpression](testConcepts.size)

    var c: Int = 0
    for (c <- 0 until testConcepts.size)
      negTestConcepts(c) = kb.getDataFactory.getOWLObjectComplementOf(testConcepts(c))

    // Classification w.r.t. all query concepts
    classification = kb.getClassMembershipResult(testConcepts, negTestConcepts, allExamples)

    def bootstrap(nFolds: Int, className: String, spark: SparkSession): Unit = {

      println()
      println(nFolds + "-fold BOOTSTRAP Experiment on ontology: ")
      // val classifierClass: Class[_] = ClassLoader.getSystemClassLoader.loadClass(className)
      val nOfConcepts: Int = if (testConcepts != null) testConcepts.size else 1

      // var Generator: Random = new Random()
      // val ntestExs: Array[Int] = Array.ofDim[Int](nFolds)

      // main loop on the folds
      for (f <- 0 until nFolds) {
        println("\n\nFold #" + (f + 1))
        println(" ******************************************************************")

        val trainingRDD: RDD[OWLIndividual] = allExamples.map { ind => allExamples.takeSample(true, 1)(0) }
        println("\nTraining examples")
        trainingRDD.foreach(println(_))

        val testRDD: RDD[OWLIndividual] = allExamples.subtract(trainingRDD)
        println("\nTest examples")
        testRDD.foreach(println(_))

        val classifier: TDTInducer = new TDTInducer(k, nOfConcepts, spark)
        // val classifier: TDTInducer = new TDTInducer(k, kb.Concepts.count().toInt, spark)
        /*  val cl: TDTInducer = (classifierClass.getConstructor(classOf[KB], classOf[Int]))
        .newInstance(kb, nOfConcepts).asInstanceOf[TDTInducer] */
        // ntestExs(f) = testRDD.count.toInt

        // training phase: using all examples but only those in the f-th partition
        println("\nTraining is starting...")

        val results: Array[Array[Int]] = k.getClassMembershipResult
        classifier.training(results, trainingRDD, testConcepts, negTestConcepts)

        val labels: Array[Array[Int]] = classifier.test(f, testRDD, testConcepts)

      } // for loop
    } // bootstrap function
  } // class
}
