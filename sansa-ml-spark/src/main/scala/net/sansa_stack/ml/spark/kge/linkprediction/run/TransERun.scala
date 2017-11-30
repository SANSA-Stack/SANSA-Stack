package net.sansa_stack.ml.spark.kge.linkprediction.run

import org.apache.spark.sql._

import org.apache.log4j.Logger
import org.apache.log4j.Level

import net.sansa_stack.ml.spark.kge.linkprediction.dataset.Dataset
import net.sansa_stack.ml.spark.kge.linkprediction.models.TransE
import net.sansa_stack.ml.spark.kge.linkprediction.prediction.Evaluate

object TransERun {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sk = SparkSession.builder.master("local")
    .appName("Tensor").getOrCreate

  def main(args: Array[String]) = {
    
    val train = new Dataset("train.txt", "\t", "false", sk)    
    val model = new TransE(train, 1, 50, "L1", sk)
 
    model.run()
    
    val test = new Dataset("test.txt", "\t", "false", sk)
    val predict = new Evaluate(model, test.df, sk)
    
    println(predict)
  
  }
  
}