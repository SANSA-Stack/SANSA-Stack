package net.sansa_stack.inference.spark.data.loader.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
  * @author Lorenz Buehmann
  */
class TurtleDataSource
  extends DataSourceRegister
    with RelationProvider
    with SchemaRelationProvider {

  override def shortName(): String = "turtle"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation =
    new TurtleRelation(parameters("path"), null)(sqlContext)

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation =
    new TurtleRelation(parameters("path"), schema)(sqlContext)
}
