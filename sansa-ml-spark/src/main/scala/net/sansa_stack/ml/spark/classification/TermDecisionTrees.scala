package net.sansa_stack.ml.spark.classification

import net.sansa_stack.owl.spark.rdd.{FunctionalSyntaxOWLAxiomsRDDBuilder, ManchesterSyntaxOWLAxiomsRDDBuilder}
import net.sansa_stack.owl.spark.rdd.OWLExpressionsRDD

import org.apache.spark.sql.SparkSession

object TermDecisionTrees {
  

  //var kb: KnowledgeBase = _
  
  def main(args: Array[String]) = {
    
    val input = "D:/Heba/Germany/DrHajira/Sources/SANSA-ML-develop/sansa-ml-spark/src/main/resources/ont_functional.owl"
    val syntax = "fun"
    
    syntax match {
      case "fun" =>
        
        println(".============================================.")
        println("| RDD OWL reader example (Functional syntax) |")
        println("`============================================´")

        val sparkSession = SparkSession.builder
          .master("local[*]")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .appName("OWL reader example (" + input + ")(Functional syntax)")
          .getOrCreate()

       val rdd = FunctionalSyntaxOWLAxiomsRDDBuilder.build(sparkSession.sparkContext, input)

       rdd.take(100).foreach(println(_))
          
          
       
        sparkSession.stop

      case "manch" =>
        
        println(".============================================.")
        println("| RDD OWL reader example (Manchester syntax) |")
        println("`============================================´")

        val sparkSession = SparkSession.builder
          .master("local[*]")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .appName("OWL reader example (" + input + ")(Manchester syntax)")
          .getOrCreate()

       //val rdd = ManchesterSyntaxOWLAxiomsRDDBuilder.build(sparkSession.sparkContext, input)
       //rdd.take(10).foreach(println(_))

        
          
     sparkSession.stop
        
      case "owl_xml" =>
        println("Not supported, yet.") 
      
      case _ =>
        println("Invalid syntax type.")
    

    }//main
  }

  
}