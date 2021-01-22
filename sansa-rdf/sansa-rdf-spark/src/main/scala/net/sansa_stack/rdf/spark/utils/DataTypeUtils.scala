package net.sansa_stack.rdf.spark.utils

import java.math.BigInteger

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

  /**
   * Central place to deal with some Spark quirks.
   *
   * - getSparkType for a [[java.math.BigInteger]] yields a Decimal(30, 0) which
   *   means that the mapping is detected. However BigInteger is not compatible with spark's Decimal.
   *   Converting the BigInteger to a BigDecimal works though.
   *
   */
  def enforceSparkCompatibility(rawValue: Any): Any = {
    val effectiveValue =
      if (rawValue != null && rawValue.isInstanceOf[BigInteger]) {
        new java.math.BigDecimal(rawValue.asInstanceOf[BigInteger])
      } else {
        rawValue
      }
    effectiveValue
  }

}
