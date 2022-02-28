package net.sansa_stack.ml.spark.classification.decisionTrees

import net.sansa_stack.ml.spark.classification.decisionTrees.ClassMembership.ClassMembership
import net.sansa_stack.owl.spark.rdd.{FunctionalSyntaxOWLAxiomsRDDBuilder, OWLAxiomsRDD}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Terminological Decision Trees
  *
  * @author Heba Mohamed
  *
  */

object TermDecisionTrees {

/*
 * The main file to call Terminological Decision Trees for Classification
 */

  def main(args: Array[String]) : Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val input : String = getClass.getResource("/trains.owl").getPath
//    val input : String = getClass.getResource("/MDM0.732.owl").getPath
  
//    val input = args(0)
    println("=============================================")
    println("       | Distributed Terminological Decision Trees |")
    println("=============================================")

    val sparkSession = SparkSession.builder
//      .master("spark://172.18.160.16:3090")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.executor.memory", "8g")
      .config("spark.executor.cores", "5")
      .appName("Terminological Decision Trees")
      .getOrCreate()

    
    
    // Call owl axiom builder to read the classes and object properties and print
    val rdd : OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(sparkSession, input)

    val kb: KB = new KB(input, rdd)
    val ClassM = new ClassMembership(kb, sparkSession)
//    val ClassName = TDTInducer.toString()
  
    val startTime = System.currentTimeMillis()

    ClassM.bootstrap(10, sparkSession)
  
    val time = System.currentTimeMillis() - startTime
  
    println("*************************************************************************")
    println("Bootstrapping done in " + (time/1000) + " sec.")
    
    
 //        val PosExamples = sparkSession.sparkContext.parallelize(
//          Array("http://example.com/foo#east1",
//            "http://example.com/foo#east2",
//            "http://example.com/foo#east3",
//            "http://example.com/foo#east4",
//            "http://example.com/foo#east5"))
//
//        val NegExamples = sparkSession.sparkContext.parallelize(
//          Array("http://example.com/foo#west6",
//            "http://example.com/foo#west7",
//            "http://example.com/foo#west8",
//            "http://example.com/foo#west9",
//            "http://example.com/foo#west10"))
//
//        val UndExamples = sparkSession.sparkContext.parallelize(new ArrayList[String]().asScala)
//
//        val numPos: Double = PosExamples.count
//        val numNeg: Double = NegExamples.count
//        val perPos: Double = numPos / (numPos + numNeg)
//        val perNeg: Double = numNeg / (numPos + numNeg)
//
//        println("\nLearning problem: \n --------------------\n")
//        println("No. of Positive examples: " + PosExamples.count)
//        println("No. of Negative examples: " + NegExamples.count)
//        println("No. of Undefined examples: " + UndExamples.count)
//        println("\nper Pos: " + perPos)
//        println("per Neg: " + perNeg)
//
//        val nGeneratedRef: Int = 50
//
//        val TDT : TDTClassifiers = new TDTClassifiers (kb, sparkSession)
//        val tree : DLTree = TDT.induceDLTree(kb.getDataFactory.getOWLThing, PosExamples, NegExamples, UndExamples, nGeneratedRef, perPos, perNeg)
//
//        val Root: OWLClassExpression = tree.getRoot()
//        println("\nRoot of the tree is: " + Root)

    /* val possubtree = tree.getPosSubTree().toString()
    println("possubtree: " + possubtree) */

    // val ind = kb.getDataFactory().getOWLNamedIndividual("http://example.com/foo#east2")
    // val classification : Int = c.classify(ind, tree)
    // println("\nclassification of east2 is " + classification)

    sparkSession.stop

  }

}
