package net.sansa_stack.ml.spark.kge.linkprediction.prediction

/**
 * Predict Abstract Class
 * ----------------------
 *
 * Created by lpfgarcia on 14/11/2017.
 */

import org.apache.spark.sql._

abstract class Predict(test: DataFrame) {

  def left(row: Row, i: Int) = {
    Row(i, row.getInt(1), row.getInt(2))
  }

  def right(row: Row, i: Int) = {
    Row(row.getInt(0), row.getInt(1), i)
  }

  def rank(row: Row, spo: String): Integer

  def ranking() = {

    var l, r = Seq[Integer]()
  
    test.collect().map { row =>
      l = rank(row, "l") +: l
      r = rank(row, "r") +: r
    }

    (l, r)
  }

  def hits10() = {

    var l, r = Seq[Boolean]()

    test.collect().map { row =>
      l = (rank(row, "l") <= 9) +: l
      r = (rank(row, "r") <= 9) +: r
    }

    (l, r)
  }

}