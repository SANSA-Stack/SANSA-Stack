package net.sansa_stack.ml.spark.classification

import java.io.File
import java.net.URI
import java.util.stream.Collectors
import scala.reflect.runtime.universe._
import scala.collection.JavaConverters._

//import net.sansa_stack.rdf.spark.io.NTripleReader
//import net.sansa_stack.inference.spark.data.loader.RDFGraphLoader


import net.sansa_stack.owl.spark.rdd.FunctionalSyntaxOWLAxiomsRDDBuilder
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

import net.sansa_stack.ml.spark.classification.KB.KB
import net.sansa_stack.ml.spark.classification.ClassMembership.ClassMembership
import net.sansa_stack.ml.spark.classification.TDTInducer.TDTInducer
import net.sansa_stack.ml.spark.classification.TDTClassifiers.TDTClassifiers

import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
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
   
    /*
 		* Call owl axion builder to read the classes and object properties and print
 		*/
 		//val parallelism = 4
 		//RDFGraphLoader.loadFromDisk(URI.create(input), sparkSession, parallelism) 
    //val triplesRDD = NTripleReader.load(sparkSession, URI.create(input))
    //triplesRDD.take(5).foreach(println(_))
  
    val rdd : OWLAxiomsRDD = FunctionalSyntaxOWLAxiomsRDDBuilder.build(sparkSession.sparkContext, input)
     
    val kb: KB = new KB(input, rdd, sparkSession)
    var ClassM = new ClassMembership(kb, sparkSession)
    val ClassName = TDTInducer.toString()
    ClassM.bootstrap(10, ClassName, sparkSession)
    
    //val PostiveExamples = sparkSession.sparkContext.parallelize(Array("east1", "east2", "east3", "east4", "east5"))
    //val NegativeExamples = sparkSession.sparkContext.parallelize(Array("west6", "west7", "west8", "west9", "west10"))
    //val c : TDTInducer = new TDTInducer(kb, kb.Concepts.count().toInt, sparkSession)
  
    sparkSession.stop

  }

}