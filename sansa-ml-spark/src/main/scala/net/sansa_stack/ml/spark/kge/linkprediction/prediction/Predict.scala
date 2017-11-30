package net.sansa_stack.ml.spark.kge.linkprediction.prediction

/**
 * Created by lpfgarcia on 14/11/2017.
 */

import org.apache.spark.sql._

abstract class Predict(test: DataFrame) {

  var left = Seq[Float]()
  var right = Seq[Float]()

  def leftRank(row: Row): Float

  def rightRank(row: Row): Float

  def ranking() = {

    test.collect().map { row =>
      left = leftRank(row) +: left
      right = rightRank(row) +: right
    }

    (left, right)
  }

  def meanRanking() {
    (left.sum / left.length,
      right.sum / right.length)
  }

}