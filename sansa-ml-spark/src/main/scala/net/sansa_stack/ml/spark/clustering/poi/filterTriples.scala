package net.sansa_stack.ml.spark.clustering.poi


import java.io.PrintWriter

import net.sansa_stack.ml.spark.clustering.datatypes.appConfig
import net.sansa_stack.ml.spark.clustering.utils.dataFiltering
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

object filterTriples {

  def main(args: Array[String]): Unit = {
    implicit val formats = DefaultFormats
    val raw_conf = scala.io.Source.fromFile("src/main/resources/conf.json").reader()
    val viennaTriplesWriter = new PrintWriter("results/vienna.nt")
    val conf = parse(raw_conf).extract[appConfig]

    // System.setProperty("hadoop.home.dir", "C:\\Hadoop") // for Windows system
    val spark = SparkSession.builder
      .master(conf.spark.master)
      .config("spark.serializer", conf.spark.spark_serializer)
      .config("spark.executor.memory", conf.spark.spark_executor_memory)
      .config("spark.driver.memory", conf.spark.spark_driver_memory)
      .config("spark.driver.maxResultSize", conf.spark.spark_driver_maxResultSize)
      .appName(conf.spark.app_name)
      .getOrCreate()
    val tomTomData = new dataFiltering(spark, conf)
    // get triples from tomtom data for poi 227585
    val (viennaTriples, viennaTriplesCategory) = tomTomData.get_triples(Array(227585, 562), tomTomData.dataRDD, spark)
    viennaTriples.collect().foreach(f => viennaTriplesWriter.println(f.getSubject.getURI + " " + f.getPredicate.getURI + " " + f.getObject.toString()))
    viennaTriplesCategory.collect().foreach(f => viennaTriplesWriter.println(f.getSubject.getURI + " " + f.getPredicate.getURI + " " + f.getObject.toString()))
    viennaTriplesWriter.close()
  }
}


