package net.sansa_stack.ml.spark.kge.linkprediction.prediction

import org.apache.spark.sql._

abstract class Predict(test: DataFrame) {

  var left: Seq[Float] = List()
  var right: Seq[Float] = List()

  def leftRank(row: Row): Float

  def rightRank(row: Row): Float

  def ranking() = {

    test.collect().map { row =>
      left = left :+ leftRank(row)
      right = right :+ rightRank(row)
    }

    (left, right)
  }

  def mean() {
    (left.sum / left.length,
      right.sum / right.length)
  }

}