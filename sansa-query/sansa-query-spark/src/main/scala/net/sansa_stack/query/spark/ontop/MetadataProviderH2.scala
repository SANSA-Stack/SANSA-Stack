package net.sansa_stack.query.spark.ontop

import it.unibz.inf.ontop.dbschema.MetadataProvider
import it.unibz.inf.ontop.dbschema.impl.OfflineMetadataProviderBuilder
import it.unibz.inf.ontop.injection.{CoreSingletons, OntopModelConfiguration}
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner}
import net.sansa_stack.rdf.common.partition.schema.{SchemaStringDate, SchemaStringDouble, SchemaStringStringType}
import net.sansa_stack.rdf.common.partition.utils.SQLUtils
import net.sansa_stack.rdf.spark.partition.core.BlankNodeStrategy
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperDoubleQuote

import scala.reflect.runtime.universe.typeOf

/**
 * Generate the JDBC metadata.
 *
 * @author Lorenz Buehmann
 */
class MetadataProviderH2(defaultConfiguration: OntopModelConfiguration) {

  val logger = com.typesafe.scalalogging.Logger("MetadataProviderH2")

  val sqlEscaper = new SqlEscaperDoubleQuote()


  def generate(partitioner: RdfPartitioner[RdfPartitionStateDefault],
               partitions: Seq[RdfPartitionStateDefault],
               blankNodeStrategy: BlankNodeStrategy.Value): MetadataProvider = {
    val builder = new OfflineMetadataProviderBuilder(defaultConfiguration.getInjector.getInstance(classOf[CoreSingletons]))
    partitions.foreach(p => generate(partitioner, p, blankNodeStrategy, builder))
    builder.build()
  }

  private def generate(partitioner: RdfPartitioner[RdfPartitionStateDefault],
                       p: RdfPartitionStateDefault,
                       blankNodeStrategy: BlankNodeStrategy.Value,
                       builder: OfflineMetadataProviderBuilder): Unit = {
    val schema = partitioner.determineLayout(p).schema

    val name = SQLUtils.createDefaultTableName(p)
    val escapedTableName = sqlEscaper.escapeTableName(name)
    p match {
      case RdfPartitionStateDefault(subjectType, predicate, objectType, datatype, langTagPresent, lang) =>
        objectType match {
          case 1 =>
            builder.createDatabaseRelation(escapedTableName,
              "s", builder.getDBTypeFactory.getDBStringType, false,
              "o", builder.getDBTypeFactory.getDBStringType, false)
          case 2 => if (langTagPresent) {
            builder.createDatabaseRelation(escapedTableName,
              "s", builder.getDBTypeFactory.getDBStringType, false,
              "o", builder.getDBTypeFactory.getDBStringType, false,
              "l", builder.getDBTypeFactory.getDBStringType, false)
          } else {
            if (schema == typeOf[SchemaStringStringType]) {
              builder.createDatabaseRelation(escapedTableName,
                "s", builder.getDBTypeFactory.getDBStringType, false,
                "o", builder.getDBTypeFactory.getDBStringType, false,
                "t", builder.getDBTypeFactory.getDBStringType, false)
            } else {
              schema match {
                case t if t =:= typeOf[SchemaStringDouble] => builder.createDatabaseRelation(escapedTableName,
                  "s", builder.getDBTypeFactory.getDBStringType, false,
                  "o", builder.getDBTypeFactory.getDBDoubleType, false)
                case t if t =:= typeOf[SchemaStringDouble] => builder.createDatabaseRelation(escapedTableName,
                  "s", builder.getDBTypeFactory.getDBStringType, false,
                  "o", builder.getDBTypeFactory.getDBDecimalType, false)
                case t if t =:= typeOf[SchemaStringDate] => builder.createDatabaseRelation(escapedTableName,
                  "s", builder.getDBTypeFactory.getDBStringType, false,
                  "o", builder.getDBTypeFactory.getDBDateType, false)
                case _ => logger.error(s"Error: couldn't create Spark table for property $predicate with schema ${schema}")
              }
            }
          }
          case _ => logger.error("TODO: bnode Spark SQL table for Ontop mappings")
        }
      case _ => logger.error("wrong partition type")
    }

  }

}
