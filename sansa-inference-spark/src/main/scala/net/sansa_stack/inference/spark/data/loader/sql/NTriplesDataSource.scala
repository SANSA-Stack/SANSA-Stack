package net.sansa_stack.inference.spark.data.loader.sql

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
  * @author Lorenz Buehmann
  */
class NTriplesDataSource
  extends DataSourceRegister
    with RelationProvider
    with SchemaRelationProvider {

  lazy val conf: Config = ConfigFactory.load("rdf_loader")

  override def shortName(): String = "ntriples"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation =
    new NTriplesRelation(parameters("path"), null, conf.getString("rdf.ntriples.parser"))(sqlContext)

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation =
    new NTriplesRelation(parameters("path"), schema, conf.getString("rdf.ntriples.parser"))(sqlContext)
}
