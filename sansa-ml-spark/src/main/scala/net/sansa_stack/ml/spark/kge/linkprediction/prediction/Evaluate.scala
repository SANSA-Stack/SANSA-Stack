package net.sansa_stack.ml.spark.kge.linkprediction.prediction

import org.apache.spark.sql._

import net.sansa_stack.ml.spark.kge.linkprediction.dataset._
import net.sansa_stack.ml.spark.kge.linkprediction.models._

class Evaluate(model: TransE, test: DataFrame, sk: SparkSession) extends Predict(test: DataFrame) {

  def head(i: Int, r: Row) = {
    Row(i, r.getInt(1), r.getInt(2))
  }

  def tail(i: Int, r: Row) = {
    Row(r.getInt(0), r.getInt(1), i)
  }

  def leftRank(row: Row) = {

    var x: Seq[Float] = List()
    val y = model.myL(row)

    x = y +: x
    for (i <- 1 until test.count().toInt) {
      x = model.myL(head(i, row)) +: x
    }

    x.sorted.indexOf(y)
  }

  def rightRank(row: Row) = {

    var x: Seq[Float] = List()
    val y = model.myL(row)

    x = y +: x
    for (i <- 1 until test.count().toInt) {
      x = model.myL(tail(i, row)) +: x
    }

    x.sorted.indexOf(y)
  }

}