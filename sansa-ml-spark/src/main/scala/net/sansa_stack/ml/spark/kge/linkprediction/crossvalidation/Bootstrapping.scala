package net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation

/**
 * Created by lpfgarcia on 24/11/2017.
 */

import org.apache.spark.sql._

class Bootstrapping(data: DataFrame) extends CrossValidation[DataFrame] {

  def crossValidation() = {
    val train = data.sample(true, 1).toDF()
    val test = data.except(train).toDF()
    (train, test)
  }

}
