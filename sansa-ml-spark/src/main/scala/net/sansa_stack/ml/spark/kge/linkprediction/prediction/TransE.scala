package net.sansa_stack.ml.spark.kge.linkprediction.prediction

/**
 * Created by lpfgarcia on 14/11/2017.
 */

import org.apache.spark.sql._

class TransE(model: net.sansa_stack.ml.spark.kge.linkprediction.models.TransE, test: DataFrame, sk: SparkSession)
    extends Predict(test: DataFrame) {

  def head(i: Int, r: Row) = {
    Row(i, r.getInt(1), r.getInt(2))
  }

  def tail(i: Int, r: Row) = {
    Row(r.getInt(0), r.getInt(1), i)
  }

  def leftRank(row: Row) = {

    var x: Seq[Float] = List()
    val y = model.myL(model.dist(row))

    x = y +: x
    for (i <- 1 until model.entities.length) {
      x = model.myL(model.dist(head(i, row))) +: x
    }

    x.sorted.indexOf(y)
  }

  def rightRank(row: Row) = {

    var x: Seq[Float] = List()
    val y = model.myL(model.dist(row))

    x = y +: x
    for (i <- 1 until model.entities.length) {
      x = model.myL(model.dist(tail(i, row))) +: x
    }

    x.sorted.indexOf(y)
  }

}