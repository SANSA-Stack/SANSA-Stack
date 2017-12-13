package net.sansa_stack.ml.spark.kge.linkprediction.convertor

/**
 * Convertor Abstract Class
 * ------------------------
 *
 * Trait for the Convertor
 *
 * Created by Hamed Shariat Yazdi
 */

import org.apache.spark.sql._

import net.sansa_stack.ml.spark.kge.linkprediction.triples._

trait Convertor {

  val (e, r) = (getEntities(), getRelations())

  def getEntities(): Array[Row]

  def getRelations(): Array[Row]

  def numeric(): Dataset[IntegerTriples]

}