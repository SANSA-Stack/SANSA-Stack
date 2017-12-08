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
import net.sansa_stack.ml.spark.kge.linkprediction.dataframe.IntegerRecord
import net.sansa_stack.ml.spark.kge.linkprediction.dataframe.StringRecord

object TransERun {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sk = SparkSession.builder.master("local")
    .appName("Tensor").getOrCreate

  def main(args: Array[String]) = {

    val table = new Triples("kge", "/home/lpfgarcia/Desktop/tensor/data/train.txt", sk)

    import sk.implicits._

    // should be removed after Hamed adaptation
    val temp = sk.createDataset(table.triples.collect().map { i =>
      StringRecord(i.getString(0), i.getString(1), i.getString(2))
    })

    print(temp.show())

    val data = new ByIndex(temp, sk)

    print(data.df.show())

    val (train, test) = new Holdout(data.df, 0.6f).crossValidation()

    println(train.show())
    println(test.show())

    //var model = new TransE(train, data.e.length, data.r.length, 100, 20, 1, "L1", sk)
    //model.run()

    //val predict = new PredictTransE(model, test).ranking()
    //println(predict)

  }

}