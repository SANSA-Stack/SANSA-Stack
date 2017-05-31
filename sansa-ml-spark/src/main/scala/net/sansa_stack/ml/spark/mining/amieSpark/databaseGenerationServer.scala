package net.sansa_stack.ml.spark.mining.amieSpark


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, _}

import net.sansa_stack.ml.spark.mining.amieSpark.DatabaseGenerator.Generator

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI


import java.io.File


object DatabaseGenerationServer {
  
  
   def deleteRecursive(path: File): Int = {

      var files = path.listFiles()
      if (files != null) {
        for (f <- files) {
          if (f.isDirectory()) {

            deleteRecursive(f)
            f.delete()
          } else {

            f.delete()

          }
        }

        path.delete()

      }

      return 0

}
  
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
    
      

 
   val sparkSession = SparkSession.builder

    .master("spark://172.18.160.16:3077")
      .appName("SPARK Reasoning")
    .config("spark.sql.warehouse.dir", "file:///data/home/MohamedMami/spark-2.1.0-bin-hadoop2.7/bin/spark-warehouse")
    
   
    .getOrCreate()
 
    
  val hdfsPath:String = "hdfs://akswnc5.aksw.uni-leipzig.de:54310/Theresa/"
   val input = "file:///data/home/TheresaNathan/shortTest.tsv"  
   // val hdfsPath = "file:////data/home/TheresaNathan/"
   // val input = "file:////data/home/TheresaNathan/test_data.tsv"
      
      
      
  val sc = sparkSession.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)


   val fs:FileSystem = FileSystem.get(new URI(hdfsPath), sc.hadoopConfiguration);
    fs.delete(new Path("precomputed0/"), true) 
   fs.delete(new Path("precomputed1/"), true) 
   fs.delete(new Path("precomputed2/"), true) 
   
   
   
   
      val algo = new Generator(hdfsPath, DfLoader.loadFromFileDF(input, sc, sqlContext, 2), 3)
  // deleteRecursive(new File ("/data/home/TheresaNathan/precomputed0/"))
  // deleteRecursive(new File ("/data/home/TheresaNathan/precomputed1/"))
  // deleteRecursive(new File ("/data/home/TheresaNathan/precomputed2/"))
 //  algo.generate(sc, sqlContext)
  

    sc.stop

  
}
}