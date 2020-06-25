package net.sansa_stack.query.spark.ontop

import net.sansa_stack.rdf.common.partition.core.RdfPartitionComplex

/**
 * @author Lorenz Buehmann
 */
object OntopMappingGenerator {

  val logger = com.typesafe.scalalogging.Logger(OntopMappingGenerator.getClass)

  def createOBDAMappingsForPartitions(partitions: Seq[RdfPartitionComplex]): String = {

    def createMapping(id: String, tableName: String, property: String): String = {
      s"""
         |mappingId     $id
         |source        SELECT "s", "o" FROM ${SQLUtils.escapeTablename(tableName)}
         |target        <{s}> <$property> <{o}> .
         |""".stripMargin
    }

    def createMappingLit(id: String, tableName: String, property: String, datatypeURI: String): String = {
      s"""
         |mappingId     $id
         |source        SELECT "s", "o" FROM ${SQLUtils.escapeTablename(tableName)}
         |target        <{s}> <$property> {o}^^<$datatypeURI> .
         |""".stripMargin
    }

    def createMappingLiteralWithType(id: String, tableName: String, property: String): String = {
      s"""
         |mappingId     $id
         |source        SELECT "s", "o", "t" FROM ${SQLUtils.escapeTablename(tableName)}
         |target        <{s}> <$property> "{o}"^^<{t}> .
         |""".stripMargin
    }

    def createMappingLang(id: String, tableName: String, property: String, lang: String): String = {
      s"""
         |mappingId     $id
         |source        SELECT "s", "o" FROM ${SQLUtils.escapeTablename(tableName)} WHERE "l" = '${lang}'
         |target        <{s}> <$property> {o}@$lang .
         |""".stripMargin
    }

    val triplesMapping =
      s"""
         |mappingId     triples
         |source        SELECT `s`, `p`, `o` FROM `triples`
         |target        <{s}> <http://sansa.net/ontology/triples> "{o}" .
         |""".stripMargin

    "[MappingDeclaration] @collection [[" +
      partitions
        .map {
          case p@RdfPartitionComplex(subjectType, predicate, objectType, datatype, langTagPresent, lang, partitioner) =>
            val tableName = SQLUtils.createTableName(p)
            val id = SQLUtils.escapeTablename(tableName + lang.getOrElse(""))
            objectType match {
              case 1 => createMapping(id, tableName, predicate)
              case 2 => if (langTagPresent) createMappingLang(id, tableName, predicate, lang.get)
                        else createMappingLit(id, tableName, predicate, datatype)
              case _ =>
                logger.error("TODO: bnode Ontop mapping creation")
                ""


            }
        }
        .mkString("\n\n") + "\n]]"
  }

}
