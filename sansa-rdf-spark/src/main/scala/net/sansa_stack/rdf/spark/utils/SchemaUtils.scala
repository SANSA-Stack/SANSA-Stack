package net.sansa_stack.rdf.spark.utils

import scala.collection.mutable.LinkedHashMap

import org.apache.spark.sql.types.{ StringType, StructField, StructType }

object SchemaUtils {

  def flattenSchemaField(
    schema: StructType,
    qualifiedName: String,
    fieldName: String, map: LinkedHashMap[String, String]) {
    val field = schema.apply(fieldName)
    val dt = field.dataType
    dt match {
      case st: StructType => flattenSchema(st, qualifiedName, map)
      case _ => map += (qualifiedName -> dt.simpleString)
    }
  }

  def flattenSchema(
    schema: StructType,
    prefix: String = "",
    map: LinkedHashMap[String, String] = LinkedHashMap[String, String]()): LinkedHashMap[String, String] = {
    schema.fields.foreach { sf =>
      val fieldName = sf.name
      val qualifiedName = prefix + (if (prefix.isEmpty()) "" else ".") + fieldName
      flattenSchemaField(schema, qualifiedName, fieldName, map)
    }
    map
  }

  /**
   * The SQL schema used for an RDF graph.
   *
   * @return the default schema for TRIPLES table with
   * s, p, o as column names.
   */
  def SQLSchemaDefault: StructType = {
    val schema = StructType(
      Seq(
        StructField("s", StringType, nullable = false),
        StructField("p", StringType, nullable = false),
        StructField("o", StringType, nullable = false)))
    schema
  }

}
