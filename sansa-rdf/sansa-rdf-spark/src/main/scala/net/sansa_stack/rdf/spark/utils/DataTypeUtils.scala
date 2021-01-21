package net.sansa_stack.rdf.spark.utils

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataType

/**
 * Utilities for working with Spark DataTypes
 */
object DataTypeUtils {

  /**
   * Return a Spark Datatype for a java class
   *
   * @param cls A java class
   * @return The Spark Datatype instance
   */
  def getSparkType(cls: Class[_]): DataType = {
    val scalaType = ScalaUtils.getScalaType(cls)
    val schema = ScalaReflection.schemaFor(scalaType)
    val sparkType = schema.dataType
    sparkType
  }

}
