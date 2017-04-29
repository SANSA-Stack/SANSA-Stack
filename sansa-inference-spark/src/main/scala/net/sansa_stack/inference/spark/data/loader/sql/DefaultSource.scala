package net.sansa_stack.inference.spark.data.loader.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType


class DefaultSource extends RelationProvider with SchemaRelationProvider {
    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String])
    : BaseRelation = {
      createRelation(sqlContext, parameters, null)
    }
    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]
                                , schema: StructType)
    : BaseRelation = {
      parameters.getOrElse("path", sys.error("'path' must be specified for our data."))
      return new NTriplesRelation(parameters.get("path").get, schema)(sqlContext)
    }
  }