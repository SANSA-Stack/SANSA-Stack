package net.sansa_stack.ml.spark.kge.linkprediction.prediction

/**
 * Predict TransE Class
 * --------------------
 *
 * Created by lpfgarcia on 14/11/2017.
 */

import org.apache.spark.sql._

import net.sansa_stack.ml.spark.kge.linkprediction.models.TransE

class PredictTransE(model: TransE, test: DataFrame) extends Predict(test: DataFrame) {

  def rank(row: Row, spo: String) = {

    var x = Seq[Float]()
    val y = model.myL(model.dist(row))

    val cor = spo match {
      case "l" => left _
      case _   => right _
    }

    x = y +: x
    for (i <- 1 to model.Ne) {
      x = model.myL(model.dist(cor(row, i))) +: x
    }

    x.sorted.indexOf(y)
  }

}