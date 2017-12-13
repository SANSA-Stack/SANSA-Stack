package net.sansa_stack.ml.spark.kge.linkprediction.run


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.util.Random
import org.apache.spark.sql.functions._


import org.springframework.util.StopWatch

import net.sansa_stack.ml.spark.kge.linkprediction.triples._
import net.sansa_stack.ml.spark.kge.linkprediction.triples.Triples
import net.sansa_stack.ml.spark.kge.linkprediction.triples.RecordStringTriples
import net.sansa_stack.ml.spark.kge.linkprediction.convertor.ByIndexConverter


object runTesting extends App {
  

  def printType[T](x: T): Unit = { println(x.getClass.toString()) }

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  

  val spark = SparkSession.builder
    .master("local")
    .appName("TransE")
    .getOrCreate()
  import spark.implicits._

//  spark.sparkContext.setLogLevel("ERROR")
  spark.sparkContext.setLogLevel("OFF")
  
 
  
  println("<<< STARTING >>>")
  
  var watch : StopWatch = new StopWatch() 
  
  watch.start()
  val trp = new Triples("train","/home/hamed/workspace/TransE/DataSets/FB15k/freebase_mtr100_mte100-train.txt",spark)
  watch.stop()
  println("Readin triples done in "+ watch.getTotalTimeSeconds + " seconds")

  watch.start()
  var num : Long = trp.triples.count()
  watch.stop()
  println("\n\n No triples = "+  num.toString() +" - Done in " + watch.getTotalTimeSeconds+" seconds.")
  
  watch.start()
  num = trp.getAllDistinctEntities().count()
  watch.stop()
  println("\n\n No Entities = "+  num.toString() +" - Done in " + watch.getTotalTimeSeconds+" seconds.")
  
  watch.start()
  num = trp.getAllDistinctPredicates().count()
  watch.stop()
  println("\n\n No Predicates = "+  num.toString() +" - Done in " + watch.getTotalTimeSeconds+" seconds.")
//  trp.getAllDistinctEntities().take(10).foreach(println)
//  println("\n \n No entities = ",trp.getAllDistinctEntities().count() )
//  println("\n \n No predicates = ",trp.getAllDistinctPredicates().count() )
  
//  val e1 = trp.getAllDistinctEntities().take(10).toSeq.toDS()
//  println("\n \n ----------")
//  e1.foreach(x=>println(x))
  val n = 10
  val conv = new ByIndexConverter(trp,spark)
  
//  val id1 = conv.entities.select("ID").sample(false,0.2).take(n)
//  val ind1 = id1.map( row => row(0).asInstanceOf[Long]).toSeq.toDS()
//  
//  val r1 = conv.getEntitiesByIndex(ind1).persist()
//  println(" count = ", r1.count)
//  r1.show()
//
//  val id2 = conv.predicates.select("ID").sample(false, 0.2).take(n)
//  val ind2 = id2.map( row => row(0).asInstanceOf[Long]).toSeq.toDS()
//  
//  val r2 = conv.getPredicatesByIndex(ind2).persist()
//  println(" count = ", r2.count)
//  r2.show()
//  
//  
  println("\n\n------ TESTING -----")
  
  lazy val smp1 = trp.triples.take(n)
  lazy val sample1 = smp1.toSeq.toDF().asInstanceOf[Dataset[RecordStringTriples]]
  
  sample1.show()
  
  val r3 = conv.getTriplesByIndex(sample1)
  r3.printSchema()
  r3.show
  
  val r4 = conv.getTriplesByString(r3)
  println("<<< DONE >>>")
}