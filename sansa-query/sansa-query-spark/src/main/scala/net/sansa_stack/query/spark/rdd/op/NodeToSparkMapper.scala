package net.sansa_stack.query.spark.rdd.op

import org.aksw.jena_sparql_api.rdf.collections.NodeMapper
import org.apache.spark.sql.types.DataType

/** Interface for mapping Jena Node's to Java objects compatible with a certain spark datatype */
trait NodeToSparkMapper {
  def getSparkDatatype(): DataType
  def getNodeMapper(): NodeMapper[Object]
}
