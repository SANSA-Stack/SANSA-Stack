package net.sansa_stack.ml.spark.kge.linkprediction.crossvalidation

import net.sansa_stack.rdf.spark.kge.triples.IntegerTriples
import org.apache.spark.sql._

/**
 * Bootstrapping
 * -------------
 *
 * Bootstrapping technique
 *
 * Created by lpfgarcia
 */
class Bootstrapping(data: Dataset[IntegerTriples])
  extends CrossValidation[Dataset[IntegerTriples]] {

  def crossValidation(): (Dataset[IntegerTriples], Dataset[IntegerTriples]) = {
    val train = data.sample(true, 1)
    val test = data.except(train)
    (train, test)
  }
}
