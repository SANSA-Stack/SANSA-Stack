package net.sansa_stack.query.spark.ontop

import scala.reflect.runtime.universe.typeOf

import it.unibz.inf.ontop.dbschema.MetadataProvider
import it.unibz.inf.ontop.dbschema.impl.OfflineMetadataProviderBuilder
import it.unibz.inf.ontop.injection.OntopModelConfiguration

import net.sansa_stack.rdf.common.partition.core.RdfPartitionComplex
import net.sansa_stack.rdf.common.partition.schema.{SchemaStringBoolean, SchemaStringDate, SchemaStringDouble, SchemaStringFloat, SchemaStringGeometry, SchemaStringStringType}
/**
 * @author Lorenz Buehmann
 */
class MetadataProviderH2(defaultConfiguration: OntopModelConfiguration) {

  val logger = com.typesafe.scalalogging.Logger("MetadataProviderH2")


  def generate(partitions: Seq[RdfPartitionComplex], blankNodeStrategy: BlankNodeStrategy.Value): MetadataProvider = {
    val builder = new OfflineMetadataProviderBuilder(defaultConfiguration.getTypeFactory)
    partitions.foreach(p => generate(p, blankNodeStrategy, builder))
    builder.build()
  }

  private def generate(p: RdfPartitionComplex, blankNodeStrategy: BlankNodeStrategy.Value,
                       builder: OfflineMetadataProviderBuilder): Unit = {
    val name = SQLUtils.createTableName(p, blankNodeStrategy)
    p match {
      case RdfPartitionComplex(subjectType, predicate, objectType, datatype, langTagPresent, lang, partitioner) =>
        objectType match {
          case 1 =>
            builder.createDatabaseRelation(SQLUtils.escapeTablename(name),
              "s", builder.getDBTypeFactory.getDBStringType, false,
              "o", builder.getDBTypeFactory.getDBStringType, false)
          case 2 => if (langTagPresent) {
            builder.createDatabaseRelation(SQLUtils.escapeTablename(name),
              "s", builder.getDBTypeFactory.getDBStringType, false,
              "o", builder.getDBTypeFactory.getDBStringType, false,
              "l", builder.getDBTypeFactory.getDBStringType, false)
          } else {
            if (p.layout.schema == typeOf[SchemaStringStringType]) {
              builder.createDatabaseRelation(SQLUtils.escapeTablename(name),
                "s", builder.getDBTypeFactory.getDBStringType, false,
                "o", builder.getDBTypeFactory.getDBStringType, false,
                "t", builder.getDBTypeFactory.getDBStringType, false)
            } else {
              p.layout.schema match {
                case t if t =:= typeOf[SchemaStringDouble] => builder.createDatabaseRelation(SQLUtils.escapeTablename(name),
                  "s", builder.getDBTypeFactory.getDBStringType, false,
                  "o", builder.getDBTypeFactory.getDBDoubleType, false)
                case t if t =:= typeOf[SchemaStringDouble] => builder.createDatabaseRelation(SQLUtils.escapeTablename(name),
                  "s", builder.getDBTypeFactory.getDBStringType, false,
                  "o", builder.getDBTypeFactory.getDBDecimalType, false)
                case t if t =:= typeOf[SchemaStringDate] => builder.createDatabaseRelation(SQLUtils.escapeTablename(name),
                  "s", builder.getDBTypeFactory.getDBStringType, false,
                  "o", builder.getDBTypeFactory.getDBDateType, false)
                case t if t =:= typeOf[SchemaStringGeometry] => builder.createDatabaseRelation(SQLUtils.escapeTablename(name),
                  "s", builder.getDBTypeFactory.getDBStringType, false,
                  "o", builder.getDBTypeFactory.getDBStringType, false)
                case _ => logger.error(s"Error: couldn't create Spark table for property $predicate with schema ${p.layout.schema}")
              }
            }
          }
          case _ => logger.error("TODO: bnode Spark SQL table for Ontop mappings")
        }
      case _ => logger.error("wrong partition type")
    }

  }

}
