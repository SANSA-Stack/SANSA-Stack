package net.sansa_stack.ml.spark.kge.linkprediction.convertor

/**
 * Convertor Abstract Class
 * ------------------------
 *
 * Created by lpfgarcia on 27/11/2017.
 */

import org.apache.spark.sql._

import net.sansa_stack.ml.spark.kge.linkprediction.triples._

abstract class Convertor(data: Dataset[StringTriples]) {

  val (e, r) = (entities(), relations())

  def entities() = {
    data.select("Subject").union(data.select("Object")).distinct().collect()
  }

  def relations() = {
    data.select("Predicate").distinct().collect()
  }

  def numeric(): Dataset[IntegerTriples]

}