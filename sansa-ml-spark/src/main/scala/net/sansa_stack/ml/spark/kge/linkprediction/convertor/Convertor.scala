package net.sansa_stack.ml.spark.kge.linkprediction.convertor

/**
 * Convertor Abstract Class
 * ------------------------
 *
 * Convertor Trait
 *
 * Created by Hamed Shariat Yazdi
 */

import org.apache.spark.sql._

import net.sansa_stack.ml.spark.kge.linkprediction.triples._

trait Convertor {

  val (e, r) = (entities(), relations())

  def entities(): Array[Row]

  def relations(): Array[Row]

  def numeric(): Dataset[IntegerTriples]

}