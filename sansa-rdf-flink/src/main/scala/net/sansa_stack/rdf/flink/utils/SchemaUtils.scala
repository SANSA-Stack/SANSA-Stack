package net.sansa_stack.rdf.flink.utils

import scala.collection.mutable.LinkedHashMap
import org.apache.calcite.avatica.ColumnMetaData.StructType
import scala.collection.JavaConversions._
import org.apache.calcite.rex.RexInputRef

object SchemaUtils {
  
  def flattenSchemaField(schema: StructType, qualifiedName: String, fieldIndx: Int, map: LinkedHashMap[String, String]) {
    val field = schema.columns.get(fieldIndx)
    val dt = field.`type`
    dt match {
      case st: StructType => flattenSchema(st, qualifiedName, map)
      case _              => map += (qualifiedName -> dt.columnClassName())
    }
  }
  def flattenSchema(schema: StructType, prefix: String = "", map: LinkedHashMap[String, String] = LinkedHashMap[String, String]()): LinkedHashMap[String, String] = {
    val fields = schema.columns.foreach { sf =>
      val fieldIndex = sf.columnName.asInstanceOf[RexInputRef].getIndex
      val fieldName = sf.columnName
      val qualifiedName = prefix + (if (prefix.isEmpty()) "" else ".") + fieldName
      flattenSchemaField(schema, qualifiedName, fieldIndex, map)
    }
    map
  }
}