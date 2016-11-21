package net.sansa_stack.spark.utils

import org.apache.spark.sql.types.StructType
import scala.collection.mutable.LinkedHashMap

object SchemaUtils {
    def flattenSchemaField(schema: StructType, qualifiedName: String, fieldName: String, map: LinkedHashMap[String, String]) {
    val field = schema.apply(fieldName)
    val dt = field.dataType
    dt match {
      case st: StructType => flattenSchema(st, qualifiedName, map)
      case _ => map += (qualifiedName -> dt.simpleString)
    }
  }

  def flattenSchema(schema: StructType, prefix: String = "", map: LinkedHashMap[String, String] = LinkedHashMap[String, String]()): LinkedHashMap[String, String] = {
    schema.fields.foreach { sf =>
      val fieldName = sf.name
      val qualifiedName = prefix + (if (prefix.isEmpty()) "" else ".") + fieldName
      flattenSchemaField(schema, qualifiedName, fieldName, map)
    }
    map
  }
}