package net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation

/**
 * Hould Out
 * ---------
 *
 * Hould Out technique
 *
 * Created by lpfgarcia
 */

import org.apache.spark.sql._

import net.sansa_stack.rdf.spark.kge.triples.IntegerTriples

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