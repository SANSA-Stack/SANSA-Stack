package net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation

/**
 * Hould Out
 * ---------
 *
 * Created by lpfgarcia on 24/11/2017.
 */

import org.apache.spark.sql._

import net.sansa_stack.ml.spark.kge.linkprediction.triples.StringTriples
import net.sansa_stack.ml.spark.kge.linkprediction.triples.IntegerTriples

case class rateException(info: String) extends Exception

class Holdout(data: Dataset[IntegerTriples], rate: Float) extends CrossValidation[Dataset[IntegerTriples]] {

  if (rate < 0 || rate >= 1)
    throw new rateException("Rate value should be higher than 0 and lower than 1")

  def crossValidation() = {
    val train = data.sample(false, rate)
    val test = data.except(train)
    (train, test)
  }

}