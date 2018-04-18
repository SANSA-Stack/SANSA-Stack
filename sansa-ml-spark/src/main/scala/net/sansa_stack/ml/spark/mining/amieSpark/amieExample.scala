package net.sansa_stack.ml.spark.mining.amieSpark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, _}
import net.sansa_stack.ml.spark.mining.amieSpark.KBObject.KB
import net.sansa_stack.ml.spark.mining.amieSpark.MineRules.Algorithm

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI


import java.io.File

object amieExample {
  
  def main(args: Array[String]) = {
    
    
    
      

  val know = new KB()
 
   val sparkSession = SparkSession.builder

    .master("local[4]")
     .appName("SPARK AMIE")
       .config("spark.eventLog.enabled", true)
       .config("spark.hadoop.validateOutputSpecs", false)
//    .config("spark.sql.warehouse.dir", "file:///data/home/MohamedMami/spark-2.1.0-bin-hadoop2.7/bin/spark-warehouse")
    .getOrCreate()
 
    
  val hdfsPath:String = args(0)
  
  val outputPath =hdfsPath
  val inputFile = args(0)//hdfsPath + args(1)
    
  
  
  
  val sc = sparkSession.sparkContext
  
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  
  know.sethdfsPath(hdfsPath)
  know.setKbSrc(inputFile)
  
  know.setKbGraph(RDFGraphLoader.loadFromFile(know.getKbSrc, sc, 2))
  know.setDFTable(DfLoader.loadFromFileDF(know.getKbSrc, sc, sqlContext, 2)  )
  
   
 
  
  
  val algo = new Algorithm (know, 0.01, 3, 0.1, hdfsPath)

    
    var erg = algo.ruleMining(sc, sqlContext)
    var outString = erg.map { x =>
      var rdfTrp = x.getRule()
      var temp = ""
      for (i <- rdfTrp.indices) {
        if (i == 0) {
          temp = rdfTrp(i) + " <= "
        } else {
          temp += rdfTrp(i) + " \u2227 "
        }
      }
      temp = temp.stripSuffix(" \u2227 ")
      temp
    }.toSeq
    
    outString.foreach(println)
    var rddOut = sc.parallelize(outString).repartition(1)

    rddOut.saveAsTextFile(outputPath + "testOut")
  
    sc.stop

  
}
  
}