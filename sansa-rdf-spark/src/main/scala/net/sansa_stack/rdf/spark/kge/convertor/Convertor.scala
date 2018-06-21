package net.sansa_stack.rdf.spark.kge.convertor

/**
 * Convertor Abstract Class
 * ------------------------
 *
 * Trait for the Convertor
 *
 * Created by Hamed Shariat Yazdi
 */

import net.sansa_stack.rdf.spark.kge.triples._
import org.apache.spark.sql._

trait Convertor {

  val (e, r) = (getEntities(), getRelations())

  def getEntities(): Array[Row]

  def getRelations(): Array[Row]

  def numeric(): Dataset[IntegerTriples]

}
