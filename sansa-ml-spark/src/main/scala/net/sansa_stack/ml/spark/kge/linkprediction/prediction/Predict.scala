package net.sansa_stack.ml.spark.kge.linkprediction.prediction

/**
 * Predict Abstract Class
 * ----------------------
 *
 * Created by lpfgarcia on 14/11/2017.
 */

import org.apache.spark.sql._

abstract class Predict(test: DataFrame) {

  var l, r = Seq[Float]()

  def left(row: Row, i: Int) = {
    Row(i, row.getInt(1), row.getInt(2))
  }

  def right(row: Row, i: Int) = {
    Row(row.getInt(0), row.getInt(1), i)
  }

  def rank(row: Row, spo: String): Float

  def ranking() = {

    test.collect().map { row =>
      l = rank(row, "l") +: l
      r = rank(row, "r") +: r
    }

    (l, r)
  }

}