package net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation

/**
 * Created by lpfgarcia on 24/11/2017.
 */

trait CrossValidation[T] {

  def crossValidation: (T, T)

}