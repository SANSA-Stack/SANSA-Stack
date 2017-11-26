package net.sansa_stack.ml.spark.classification

import java.io.PrintStream
//import java.lang.reflect.InvocationTargetException
import java.util.ArrayList
import java.util.HashSet
import java.util.Set
import scala.util.Random
import collection.JavaConverters._
import scala.collection

import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLIndividual
import org.semanticweb.owlapi.model.OWLNamedIndividual

import net.sansa_stack.ml.spark.classification
import net.sansa_stack.ml.spark.classification.TDTInducer.TDTInducer
import net.sansa_stack.ml.spark.classification.KB.KB
import net.sansa_stack.ml.spark.classification.ConceptsGenerator._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ClassMembership {

    protected var kb: KB = _
    protected var allExamples: RDD[OWLIndividual] = _
    protected var testConcepts: Array[OWLClassExpression] = _
    protected var negTestConcepts: Array[OWLClassExpression] = _
    protected var classification: RDD[Array[Int]] = _
   // val NQuery: Int = 30
   // val Threshold: Double = 0.95
/**
  * Implementing class-membership
	*/
class ClassMembership (k: KB, sc: SparkSession){

  //var console: PrintStream = System.out

    kb = k
    allExamples = kb.getIndividuals
    val cg: ConceptsGenerator = new ConceptsGenerator(kb)
    testConcepts = cg.generateQueryConcepts(1, sc) 
    negTestConcepts = Array.ofDim[OWLClassExpression](testConcepts.size)
    
    var c: Int = 0
    for (c <- 0 until testConcepts.size)
      negTestConcepts(c)= kb.getDataFactory.getOWLObjectComplementOf(testConcepts(c))
    
  //     negTestConcepts = testConcepts.map(x => kb.getDataFactory().getOWLObjectComplementOf(testConcepts))
    // Classification wrt all query concepts
    //println("\nClassifying all examples ------ ")
    kb.getReasoner
  
    classification = kb.getClassMembershipResult(testConcepts, negTestConcepts, allExamples)

    
 def bootstrap(nFolds: Int, className: String, spark: SparkSession): Unit = {
   
      println(nFolds + "-fold BOOTSTRAP Experiment on ontology: ")
      val classifierClass: Class[_] = ClassLoader.getSystemClassLoader.loadClass(className)
      val nOfConcepts: Int = if (testConcepts != null) testConcepts.size else 1
      
      var Generator: Random = new Random()
      val ntestExs: Array[Int] = Array.ofDim[Int](nFolds)
    
    // main loop on the folds
      for (f <- 0 until nFolds) {
        println("\n\nFold #" + f)
        println(" ******************************************************************")
        val trainingExsSet: Set[Integer] = new HashSet[Integer]()
        var trainRDD = spark.sparkContext.parallelize(trainingExsSet.asScala.toSeq)
        
        val testingExsSet: Set[Integer] = new HashSet[Integer]()
        var testRDD = spark.sparkContext.parallelize(testingExsSet.asScala.toSeq)
        
        var rand1 = new ArrayList[Integer]
        for (r <- 0 until allExamples.count.toInt)
            rand1.add(Generator.nextInt(allExamples.count.toInt))  
    
        var newRDD = spark.sparkContext.parallelize(rand1.asScala)
        trainRDD.union(newRDD) 
        //trainingExsSet.add(Generator.nextInt(allExamples.count.toInt))
         
        var r = 0 to allExamples.count.toInt
        var rand2 = spark.sparkContext.parallelize(r)
        
        if (!trainRDD.collect().contains(rand2))
          testRDD.union(rand2.asInstanceOf[RDD[Integer]])
         
         /*for (r <- 0 until allExamples.count.toInt){
           if (!trainRDD.collect().contains(r))
              testRDD.union(r)
         }*/
           
        
        /*var trainingExs: Array[Integer] = Array.ofDim[Integer](0)
        var testExs: Array[Integer] = Array.ofDim[Integer](0)
        
        trainingExs = trainingExsSet.toArray(trainingExs)
        testExs = testingExsSet.toArray(testExs)*/
        
        ntestExs(f) = testRDD.count.toInt
  
        // training phase: using all examples but those in the f-th partition
        println("Training is starting...")
        
        val cl : TDTInducer = new TDTInducer(kb, kb.Concepts.count().toInt, spark)
        
        /*val cl: TDTInducer = (classifierClass.getConstructor(classOf[KB], classOf[Int]))
        .newInstance(kb, nOfConcepts).asInstanceOf[TDTInducer]*/
        
        val results: Array[Array[Int]] = kb.getClassMembershipResult
       // cl.training(results, trainingExs, testConcepts, negTestConcepts)
       cl.training(results, trainRDD, testConcepts, negTestConcepts)
  
  
        //val labels: Array[Array[Int]] = cl.test(f, testRDD, testConcepts)

    }

  }// for loop

 }
}