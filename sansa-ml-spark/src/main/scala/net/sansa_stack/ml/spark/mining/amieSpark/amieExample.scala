package net.sansa_stack.ml.spark.mining.amieSpark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, _}
import net.sansa_stack.ml.spark.mining.amieSpark.KBObject.KB
import net.sansa_stack.ml.spark.mining.amieSpark.MineRules.Algorithm

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI

object amieExample {
  
  def main(args: Array[String]) = {
    
    /*
     * config:
     * .config("spark.executor.memory", "20g")
     * .config("spark.driver.maxResultSize", "20g")
     * .config("spark.sql.autoBroadcastJoinThreshold", "300000000")
     * .config("spark.sql.shuffle.partitions", "100")
     * .config("spark.sql.warehouse.dir", "file:///C:/Users/Theresa/git/Spark-Sem-ML/inference-spark/spark-warehouse")
     * .config("spark.sql.crossJoin.enabled", true)
     */
    
      

  val know = new KB()
 
   val sparkSession = SparkSession.builder

    .master("spark://172.18.160.16:3077")
      .appName("SPARK Reasoning")
    .config("spark.sql.warehouse.dir", "/spark-2.1.0-bin-hadoop2.7/bin/spark-warehouse")
    
   
    .getOrCreate()
 
    
  val hdfsPath:String = "<hdfspath>"
   // val hdfsPath = "file:///data/home/TheresaNathan/"  
  val outputPath = "file:///data/home/TheresaNathan/"  
  val inputFile = "/shortTest.tsv"    
  
  
  
  val sc = sparkSession.sparkContext
  
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  
  know.sethdfsPath(hdfsPath)
  know.setKbSrc(inputFile)
  
  know.setKbGraph(RDFGraphLoader.loadFromFile(know.getKbSrc(), sc, 2))
  know.setDFTable(DfLoader.loadFromFileDF(know.getKbSrc, sc, sqlContext, 2)  )
  /*
   val fs:FileSystem = FileSystem.get(new URI("hdfs://2001:638:902:2007:92b1:1cff:fefd:7e26:54310/Theresa/"), sc.hadoopConfiguration);
    fs.delete(new Path("precomputed0/"), true) 
   fs.delete(new Path("precomputed1/"), true) 
   
    
    */
  
  val algo = new Algorithm (know, 0.01, 3, 0.1, hdfsPath)

    
    var erg = algo.ruleMining(sc, sqlContext)
    var outString = erg.map { x =>
      var rdfTrp = x.getRule()
      var temp = ""
      for (i <- 0 to rdfTrp.length - 1) {
        if (i == 0) {
          temp = rdfTrp(i) + " <= "
        } else {
          temp += rdfTrp(i) + " \u2227 "
        }
      }
      temp = temp.stripSuffix(" \u2227 ")
      temp
    }.toSeq
    var rddOut = sc.parallelize(outString)

    rddOut.saveAsTextFile(outputPath + "/testOut")
  
    sc.stop

  
}
  
}