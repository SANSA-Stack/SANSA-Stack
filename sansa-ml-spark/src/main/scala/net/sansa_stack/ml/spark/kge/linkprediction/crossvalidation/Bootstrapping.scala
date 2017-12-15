package net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation

/**
 * Bootstrapping
 * -------------
 *
 * Bootstrapping technique
 *
 * Created by lpfgarcia
 */

import org.apache.spark.sql._

import net.sansa_stack.rdf.spark.kge.triples.IntegerTriples

class Bootstrapping(data: Dataset[IntegerTriples])
    extends CrossValidation[Dataset[IntegerTriples]] {

  def crossValidation() = {
    val train = data.sample(true, 1)
    val test = data.except(train)
    (train, test)
  }

}