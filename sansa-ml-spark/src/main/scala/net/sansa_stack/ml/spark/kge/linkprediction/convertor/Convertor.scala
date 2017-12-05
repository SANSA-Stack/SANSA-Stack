package net.sansa_stack.ml.spark.kge.linkprediction.convertor

/**
 * Convertor Abstract Class
 * ------------------------
 *
 * Created by lpfgarcia on 27/11/2017.
 */

import org.apache.spark.sql._

abstract class Convertor(data: DataFrame) {

  def numeric(): DataFrame

}