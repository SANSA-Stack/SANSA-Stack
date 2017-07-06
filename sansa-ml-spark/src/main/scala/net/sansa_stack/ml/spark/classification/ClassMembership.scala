package net.sansa_stack.ml.spark.classification

import java.io.PrintStream
import java.lang.reflect.InvocationTargetException
import java.util.ArrayList
import java.util.HashSet
import java.util.Set
import scala.util.Random
import collection.JavaConverters._
import scala.collection

import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLIndividual

import net.sansa_stack.ml.spark.classification
import net.sansa_stack.ml.spark.classification.TDTInducer.TDTInducer
import net.sansa_stack.ml.spark.classification.KB.KB
import net.sansa_stack.ml.spark.classification._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ClassMembership {

  protected var kb: KB = _
  
   val sparkSession = SparkSession.builder
          .master("local[*]")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .appName("Class Membership")
          .getOrCreate()
          
    val conf = new SparkConf().setAppName("ClassMembership").setMaster("local[*]")  // local mode
    val sc = new SparkContext(conf)

//	private String urlOwlFile;
  val QUERY_NB: Int = 30
  val THRESHOLD: Double = 0.05


/**
  * Implementing class-membership with ENN
	*/
class ClassMembership {

  protected var allExamples: RDD[OWLIndividual] = _
  protected var testConcepts: Array[OWLClassExpression] = _
  protected var negTestConcepts: Array[OWLClassExpression] = _
  protected var classification: RDD[Array[Int]] = _
    
  var console: PrintStream = System.out

  def this(k: KB) = {
    this()
    kb = k
    allExamples = kb.getIndividuals
    val cg: ConceptsGenerator = new ConceptsGenerator(kb)
    testConcepts = cg.generateQueryConcepts(1) 
    negTestConcepts = Array.ofDim[OWLClassExpression](testConcepts.length)
    
  
    for (c <- 0 until testConcepts.length)
      negTestConcepts(c) = kb.getDataFactory.getOWLObjectComplementOf(testConcepts(c))
  
      // Classification wrt all query concepts
      println("\nClassifying all examples ------ ")
      kb.getReasoner
  
      classification = kb.getClassMembershipResult(testConcepts, negTestConcepts, allExamples)
  }


 def bootstrap(nFolds: Int, className: String): Unit = {
    println(nFolds + "-fold BOOTSTRAP Experiment on ontology: ")
    val classifierClass: Class[_] = ClassLoader.getSystemClassLoader.loadClass(className)
    val nOfConcepts: Int = if (testConcepts != null) testConcepts.length else 1
    
    var Generator: Random = new Random()

    // main loop on the folds
    val ntestExs: Array[Int] = Array.ofDim[Int](nFolds)
    for (f <- 0 until nFolds) {
      println("\n\nFold #" + f)
      println(" ******************************************************************")
      val trainingExsSet: Set[Integer] = new HashSet[Integer]()
      val testingExsSet: Set[Integer] = new HashSet[Integer]()
      
      for (r <- 0 until allExamples.count.asInstanceOf[Int])
        trainingExsSet.add(Generator.nextInt(allExamples.count.asInstanceOf[Int]))
      
      for (r <- 0 until allExamples.count.asInstanceOf[Int] if !trainingExsSet.contains(r))
        testingExsSet.add(r)
      
      var trainingExs: Array[Integer] = Array.ofDim[Integer](0)
      var testExs: Array[Integer] = Array.ofDim[Integer](0)
      
      trainingExs = trainingExsSet.toArray(trainingExs)
      testExs = testingExsSet.toArray(testExs)
      
      ntestExs(f) = testExs.length

      // training phase: using all examples but those in the f-th partition
      println("Training is starting...")
      
      val cl: TDTInducer = (classifierClass.getConstructor(classOf[KB], classOf[Int]))
      .newInstance(kb, nOfConcepts.asInstanceOf[Object]).asInstanceOf[TDTInducer]
      
      val results: Array[Array[Int]] = kb.getClassMembershipResult
      cl.training(results, trainingExs, testConcepts, negTestConcepts)

     val labels: Array[Array[Int]] = cl.test(f, testExs, testConcepts)

    }

  }// for f - fold look

 }
}