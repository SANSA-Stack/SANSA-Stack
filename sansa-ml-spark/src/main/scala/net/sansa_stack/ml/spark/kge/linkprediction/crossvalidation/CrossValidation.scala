package net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation

/**
 * Cross Validation Techniques
 * ---------------------------
 *
 * Trait for the Cross Validation techniques
 *
 * Created by lpfgarcia
 */

trait CrossValidation[T] {

  def crossValidation: (T, T)

}