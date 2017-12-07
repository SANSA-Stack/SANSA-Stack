package net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation

/**
 * Bootstrapping
 * -------------
 *
 * Created by lpfgarcia on 24/11/2017.
 */

import org.apache.spark.sql._

import net.sansa_stack.ml.spark.kge.linkprediction.dataframe._

class Bootstrapping(data: Dataset[IntegerRecord]) extends CrossValidation[Dataset[IntegerRecord]] {

  def crossValidation() = {
    val train = data.sample(true, 1)
    val test = data.except(train)
    (train, test)
  }

}
