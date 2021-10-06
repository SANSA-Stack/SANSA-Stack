package net.sansa_stack.query.spark.rdd.op

import org.aksw.jena_sparql_api.rdf.collections.NodeMapper
import org.apache.spark.sql.types.DataType

case class NodeToSparkMapperImpl(sparkDatatype: DataType, nodeMapper: NodeMapper[Object])
  extends NodeToSparkMapper with Serializable
{
  override def getSparkDatatype(): DataType = sparkDatatype
  override def getNodeMapper(): NodeMapper[Object] = nodeMapper
}
