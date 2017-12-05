package net.sansa_stack.ml.spark.kge.linkprediction.run

/**
 * Created by lpfgarcia on 14/11/2017.
 */

import org.apache.spark.sql._

import org.apache.log4j.Logger
import org.apache.log4j.Level

import net.sansa_stack.ml.spark.kge.linkprediction.dataframe.Triples
import net.sansa_stack.ml.spark.kge.linkprediction.convertor.ByIndex
import net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation.Holdout
import net.sansa_stack.ml.spark.kge.linkprediction.prediction.PredictTransE
import net.sansa_stack.ml.spark.kge.linkprediction.models.TransE

object TransERun {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sk = SparkSession.builder.master("local")
    .appName("Tensor").getOrCreate

  def main(args: Array[String]) = {

    val table = new Triples("kge", "/home/lpfgarcia/Desktop/tensor/data/train.txt", sk)
    val data = new ByIndex(table.triples, sk)

    val (train, test) = new Holdout(data.df, 0.6f).crossValidation()

    println(train.show())
    println(test.show())

    var model = new TransE(train, data.e.length, data.r.length, 100, 20, 1, "L1", sk)
    model.run()

    val predict = new PredictTransE(model, test).ranking()
    println(predict)

  }

}