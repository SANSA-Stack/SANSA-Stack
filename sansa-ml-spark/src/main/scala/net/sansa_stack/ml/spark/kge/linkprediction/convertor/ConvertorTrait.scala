package net.sansa_stack.ml.spark.kge.linkprediction.convertor

/**
 * Converter Trait
 * 
 * Created by: Hamed Shariat Yazdi
 * 
 */

import org.apache.spark.sql._
import net.sansa_stack.ml.spark.kge.linkprediction.triples.RecordStringTriples
import net.sansa_stack.ml.spark.kge.linkprediction.triples.RecordLongTriples

trait ConvertorTrait {
  
  def getTriplesByIndex(dsTriplesInString : Dataset[RecordStringTriples]) : Dataset[RecordLongTriples]
  
  def getTriplesByString(dsTriplesInLong : Dataset[RecordLongTriples]) : Dataset[RecordStringTriples]
  
  def getEntitiesByIndex(dsEntitiesIndices : Dataset[Long]) : Dataset[(String,Long)]
  
  def getPredicatesByIndex(dsPredicatesIndicies : Dataset[Long]) : Dataset[(String,Long)]
}