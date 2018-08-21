package net.sansa_stack.ml.spark.kge.linkprediction.run

/**
 * Created by lpfgarcia on 14/11/2017.
 */

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql._

import net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation.{ kFold, Bootstrapping, Holdout }
import net.sansa_stack.ml.spark.kge.linkprediction.models.TransE
import net.sansa_stack.ml.spark.kge.linkprediction.prediction.PredictTransE
import net.sansa_stack.rdf.spark.kge.convertor.ByIndex
import net.sansa_stack.rdf.spark.kge.triples._

object TransERun {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder.master("local")
    .appName("kge").getOrCreate

  def main(args: Array[String]): Unit = {

    val table = new Triples("/home/lpfgarcia/Desktop/SANSA-ML/data/train.txt", "\t", false, false, spark)

    print(table.triples.show())

    val data = new ByIndex(table.triples, spark)

    print(data.triples.show())

    val (train, test) = new Holdout(data.triples, 0.6f).crossValidation()

    println("Trinamento:")
    println(train.show())
    println("Teste:")
    println(test.show())

    // var model = new TransE(train, data.e.length, data.r.length, 100, 20, 1, "L1", spark)
    // model.run()

    // val predict = new PredictTransE(model, test).ranking()
    // println(predict)
  }
}
