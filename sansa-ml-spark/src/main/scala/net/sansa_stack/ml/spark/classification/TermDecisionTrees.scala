package net.sansa_stack.ml.spark.classification 

import java.util.ArrayList
import scala.reflect.runtime.universe._
import scala.collection.JavaConverters._
import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLIndividual

import net.sansa_stack.ml.spark.classification.KB.KB
import net.sansa_stack.ml.spark.classification.ClassMembership.ClassMembership
import net.sansa_stack.ml.spark.classification.TDTClassifiers.TDTClassifiers

import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object TermDecisionTrees {

  /*
 * The main file to call Terminological Decision Trees for Classification 
 */

  def main(args: Array[String]) = {

    val input = "src/main/resources/Classification/trains.owl"

    println("=================================")
    println("|  Termnological Decision Tree  |")
    println("=================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "net.sansa_stack.ml.spark.classification.Registrator")
      .appName("Termnological Decision Tree")
      .getOrCreate()
   
    //Call owl axion builder to read the classes and object properties and print
      
 	 	val rdd : OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(sparkSession.sparkContext, input)
   
    val kb: KB = new KB(input, rdd, sparkSession)
    var ClassM = new ClassMembership(kb, sparkSession)
    val ClassName = TDTInducer.toString()
    ClassM.bootstrap(10, ClassName, sparkSession)

    //val c : TDTInducer = new TDTInducer(kb, kb.Concepts.count().toInt, sparkSession)
    
//    var PosExamples = sparkSession.sparkContext.parallelize(Array("http://example.com/foo#east1", 
//        "http://example.com/foo#east2",
//        "http://example.com/foo#east3",
//        "http://example.com/foo#east4",
//        "http://example.com/foo#east5"))
//    
//    var NegExamples = sparkSession.sparkContext.parallelize(Array("http://example.com/foo#west6", 
//        "http://example.com/foo#west7", 
//        "http://example.com/foo#west8", 
//        "http://example.com/foo#west9", 
//        "http://example.com/foo#west10"))
//    
//    var UndExamples = sparkSession.sparkContext.parallelize(new ArrayList[String]().asScala)
//
//    val numPos: Double = PosExamples.count
//    val numNeg: Double = NegExamples.count
//    val perPos: Double = numPos / (numPos + numNeg)
//    val perNeg: Double = numNeg / (numPos + numNeg)
//    
//    println("\nLearning problem: \n --------------------\n")
//    println("No. of Positive examples: " + PosExamples.count)
//    println("No. of Negative examples: " + NegExamples.count)
//    println("No. of Undefined examples: " + UndExamples.count)
//    println("\nper Pos: " + perPos)
//    println("per Neg: " + perNeg)
//    
//    val nGeneratedRef: Int = 50
//    
//    val c : TDTClassifiers = new TDTClassifiers (kb, sparkSession)
//    val tree : DLTree = c.induceDLTree(kb.getDataFactory.getOWLThing, PosExamples, NegExamples, UndExamples, nGeneratedRef, perPos, perNeg)
//    
//    val Root: OWLClassExpression = tree.getRoot()
//    println("\nRoot of the tree is: " + Root)
    
    /*val possubtree = tree.getPosSubTree().toString()
    println("possubtree: " + possubtree)*/
    
    //val ind = kb.getDataFactory().getOWLNamedIndividual("http://example.com/foo#east2") 
    //val classification : Int = c.classify(ind, tree) 
    //println("\nclassification of east2 is " + classification)
    
    sparkSession.stop

  }

}