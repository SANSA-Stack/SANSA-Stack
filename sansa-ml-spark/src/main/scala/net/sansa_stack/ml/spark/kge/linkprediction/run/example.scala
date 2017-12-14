package net.sansa_stack.ml.spark.kge.linkprediction.run

import org.apache.spark.sql._
import net.sansa_stack.rdf.spark.kge.triples.Triples
import net.sansa_stack.rdf.spark.kge.convertor.ByIndex
import net.sansa_stack.rdf.spark.kge.crossvalidation.Holdout

object example extends App {

  val spark = SparkSession.builder.master("local")
    .appName("kge").getOrCreate
    
  // Assuming the file "test.txt" exists which contains the triples, we load the file 
  // the function is provided with the path of the file 
  // delimiter = "\t" , indicates file is in tab separated format
  // headers = false , indicates that we ignore the header of the file 
  // numeric = false , indicated that content of the file in not in numeric format i.e. (subject,predicate,object) are not numerically given   
  
  val data = new Triples("/home/hamed/Downloads/FB15K-237.2/test.txt","\t", false, false, spark)
  
  // getting 10 distinct entities in (s,p,o) i.e. subjects + objects and printing them
  data.getEntities().take(10).foreach(println)
  
  // getting 10 distinct predicates in (s,p,o) and printing them
  data.getEntities().take(10).foreach(println)
  
  // converting the original data to indexData 
  val indexedData = new ByIndex(data.triples, spark)
  
  val rumericalData = indexedData.numeric()
  
  // getting 10 distinct (s,p,o) in their numeric (indexed) form and print them
  indexedData.numeric.take(10).foreach(println)
  
  // using the holdout cross validation technique to get 60% of data as training and the rest as testing  
  val (train, test) = new Holdout(rumericalData, 0.6f).crossValidation()
  
  println("<< DONE >>")
}