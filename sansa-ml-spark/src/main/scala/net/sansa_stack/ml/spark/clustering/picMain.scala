package net.sansa_stack.ml.spark.clustering
import org.apache.jena.riot.Lang
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.graph.GraphLoader
//import net.sansa_stack.ml.spark.clustering.RDFGraphPowerIterationClustering
object picMain {
    def main(args: Array[String]): Unit =  {
   
    val spark = SparkSession.builder
      .appName(s"Power Iteration Clustering example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    System.setProperty("spark.akka.frameSize", "2000")
    Logger.getRootLogger.setLevel(Level.ERROR)
    val input = "/home/hajira/Desktop/Link to Supervision/Tina/dbpedia_2015-10.nt" //hdfs://172.18.160.17:54310/TinaBoroukhian/input/HairStylist_TaxiDriver.txt" 
    val output = "src/main/resources/PICOut"
    
    println("============================================")
    println("| Power Iteration Clustering   example     |")
    println("============================================")
    val k =2
    val maxIterations =5

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)

    val graph = triples.asStringGraph()

    //RDFGraphPowerIterationClustering(spark, graph, output, outputevl, outputsim, k, maxIterations)
    val cluster = RDFGraphPowerIterationClustering.apply(spark, graph, output, k, maxIterations)
    cluster.collect().foreach(println)
    spark.stop

  }
}
