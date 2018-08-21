package net.sansa_stack.ml.spark.kge.linkprediction.evaluate

/**
 * Evaluate Object
 * ---------------
 *
 * Created by lpfgarcia on 05/12/2017.
 */

object Evaluate {

  def meanRank(left: Array[Float], right: Array[Float]): (Float, Float) = {
    (left.sum / left.length,
      right.sum / right.length)
  }
}
