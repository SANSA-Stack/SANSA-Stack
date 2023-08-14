package net.sansa_stack.ml.spark.classification.decisionTrees

import net.sansa_stack.ml.spark.classification.decisionTrees.ConceptsGenerator._
import net.sansa_stack.ml.spark.classification.decisionTrees.DistTDTInducer.DistTDTInducer
import net.sansa_stack.ml.spark.classification.decisionTrees.OWLReasoner.StructuralReasoner.StructuralReasoner
import net.sansa_stack.ml.spark.classification.decisionTrees.PerformanceMetrics.MetricsComputation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLIndividual

object ClassMembership {
  
  /**
   *  Implementing class-membership
   *
   *  @param k Knowledgebase
   */

  class ClassMembership (k: KB) {

   /**
     *  Bootstrap function divides the individuals into training and testing ones
     *  then apply training and testing phases
     *
     *  @param noFolds number of folds
     *  @param spark Spark Session
     */

    var classifications: RDD[((OWLClassExpression, OWLIndividual), Int)] = _
  
    def bootstrap(noFolds: Int, spark: SparkSession): Unit = {
  
      var seed = 2
      val kb: KB = k
      val allExamples = kb.getIndividuals
      val cg: ConceptsGenerator = new ConceptsGenerator(kb)
      val testConcepts: Array[OWLClassExpression] = cg.generateQueryConcepts(1)
      val negTestConcepts: Array[OWLClassExpression] = Array.ofDim[OWLClassExpression](testConcepts.length)
  
      for (c <- testConcepts.indices)
        negTestConcepts(c) = kb.getDataFactory.getOWLObjectComplementOf(testConcepts(c))
  
      // Classification w.r.t. all query concepts
      val classification = kb.KB.getClassMembershipResult(testConcepts, negTestConcepts, allExamples)
      
      println("******************************************************* ")
      println("    BOOTSTRAP Experiment of " + noFolds + "-folds")
      println("******************************************************* ")
      
      val noConcepts: Int =
        if (testConcepts != null) testConcepts.length
        else 1
      
      val performanceMetrics = new MetricsComputation(noConcepts, noFolds)
  
      val cl : DistTDTInducer = new DistTDTInducer(kb, kb.concepts.count().toInt, spark)
//      val classifier = new TDTClassifiers.TDTClassifiers(kb, spark)
//      val classifier = new TDTClassifier2.TDTClassifier2(kb, spark)

      
      for (f <- 1 to noFolds) {
        
        println("\n**************** Fold #" + f + "  ****************")
   
        val split: Array[RDD[OWLIndividual]] = allExamples.randomSplit(Array(0.7, 0.3), seed)
        val trainData: RDD[OWLIndividual] = split(0)
        val testData: RDD[OWLIndividual] = split(1)
        
        // training phase: using all examples but those in the f-th partition
        println("Training is starting...")
        
//        val result = kb.getClassMembershipResults
        
        cl.training(classification, trainData, testConcepts, negTestConcepts)
        
        val testing = cl.test(f, testData, testConcepts)
        
        seed += 1
 
        performanceMetrics.computeMetricsPerFold(f, testing, classification, testData)
      } // fold Loop
      
      performanceMetrics.computeOverAllResults(noConcepts)
//      println("Overall complexity = " + classifier.getNodes + " nodes generated")
    } // bootstrap
  
  

  } // class
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
