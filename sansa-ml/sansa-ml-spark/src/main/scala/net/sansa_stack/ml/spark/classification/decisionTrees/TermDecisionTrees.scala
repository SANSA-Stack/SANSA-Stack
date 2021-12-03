package net.sansa_stack.ml.spark.classification.decisionTrees

import java.util.ArrayList

import net.sansa_stack.ml.spark.classification.decisionTrees.KB
import net.sansa_stack.ml.spark.classification.decisionTrees.ClassMembership.ClassMembership
import net.sansa_stack.ml.spark.classification.decisionTrees.TDTClassifiers.TDTClassifiers
import net.sansa_stack.ml.spark.classification.decisionTrees.TDTInducer.TDTInducer
import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLIndividual

import scala.reflect.runtime.universe._
import scala.collection.JavaConverters._
import scopt.OptionParser

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

    println("=============================================")
    println("| Distributed Terminological Decision Trees |")
    println("=============================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 //     .config("spark.kryo.registrator", "net.sansa_stack.ml.spark.classification.Registrator")
      .appName("Terminological Decision Trees")
      .getOrCreate()

    // Call owl axiom builder to read the classes and object properties and print
    val rdd : OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(sparkSession, input)

    val kb: KB = new KB(input, rdd, sparkSession)
    val ClassM = new ClassMembership(kb, sparkSession)
    val ClassName = TDTInducer.toString()
  //  ClassM.bootstrap(10, ClassName, sparkSession)
 //   val c = new TDTInducer(kb, kb.concepts.count().toInt, sparkSession)

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
