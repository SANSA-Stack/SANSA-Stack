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


//     val input : String = getClass.getResource("/finance.owl").getPath
      val input : String = getClass.getResource("/trains.owl").getPath
//    val input : String = getClass.getResource("/MDM0.732.owl").getPath

//    val input = args(0)
    println("=============================================")
    println("|  Distributed Terminological Decision Trees |")
    println("=============================================")
    
    val sparkSession = SparkSession.builder
//      .master("spark://172.18.160.16:3090")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("spark.executor.memory", "8g")
      .config("spark.executor.cores", "5")
      .appName("Terminological Decision Trees")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    
    // Call owl axiom builder to read the classes and object properties and print
    val rdd : OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(sparkSession, input)

    val kb: KB = new KB(input, rdd, sparkSession)
//    val reasoner = new StructuralReasoner(sparkSession, kb)
//    val instances: Int = reasoner.getInstances(kb.getDataFactory.getOWLClass("http://acl/BMV#" + "IncreasedRiskEndometrialCancer"), b = false).size()
//    println("number of instances are  " + instances)
    val ClassM = new ClassMembership(kb)
//    val ClassName = TDTInducer.toString()
  
    val startTime = System.currentTimeMillis()

    ClassM.bootstrap(10, sparkSession)
  
    val time = System.currentTimeMillis() - startTime
  
    println("*************************************************************************")
    println("Bootstrapping done in " + (time/1000) + " sec.")
    
    sparkSession.stop

  }

}
