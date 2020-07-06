package net.sansa_stack.query.spark.ontop

import scala.reflect.macros.whitebox

import net.sansa_stack.rdf.common.partition.core.RdfPartitionComplex

/**
 * @author Lorenz Buehmann
 */
object OntopMappingGenerator {

  val logger = com.typesafe.scalalogging.Logger(OntopMappingGenerator.getClass)

  val blankNodeStrategy: BlankNodeStrategy.Value = BlankNodeStrategy.Table

  def createOBDAMappingsForPartitions(partitions: Set[RdfPartitionComplex]): String = {

    // object is URI or bnode
    def createMapping(id: String, tableName: String, partition: RdfPartitionComplex): String = {

      val targetSubject = if (partition.subjectType == 0) "_:{s}" else "<{s}>"
      val targetObject = if (partition.objectType == 0) "_:{o}" else "<{o}>"

      if (blankNodeStrategy == BlankNodeStrategy.Table) {
        s"""
           |mappingId     $id
           |source        SELECT "s", "o" FROM ${SQLUtils.escapeTablename(tableName)}
           |target        $targetSubject <${partition.predicate}> $targetObject .
           |""".stripMargin
      } else if (blankNodeStrategy == BlankNodeStrategy.Column) {
        val whereCondition = s"${if (partition.subjectType == 1) "NOT" else ""} s_blank AND" +
          s" ${if (partition.objectType == 1) "NOT" else ""} o_blank"

        s"""
           |mappingId     $id
           |source        SELECT "s", "o" FROM ${SQLUtils.escapeTablename(tableName)} WHERE $whereCondition
           |target        $targetSubject <${partition.predicate}> $targetObject .
           |""".stripMargin
      } else {
        throw new RuntimeException(s"Unsupported Blank Node Strategy:$blankNodeStrategy")
      }
    }

    // object is string literal
    def createMappingStringLit(id: String, tableName: String, partition: RdfPartitionComplex): String = {
      val targetSubject = if (partition.subjectType == 0) "_:{s}" else "<{s}>"
      val targetObject = if (partition.langTagPresent) s"{o}@${partition.lang.get}" else "{o}"
      val whereConditionLang = if (partition.langTagPresent) s"'$partition.lang'" else "NULL"

      s"""
         |mappingId     $id
         |source        SELECT "s", "o" FROM ${SQLUtils.escapeTablename(tableName)} WHERE "l" = ${whereConditionLang}
         target        $targetSubject <${partition.predicate}> $targetObject .
         |""".stripMargin
    }

    // object is other literal
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
            val tableName = SQLUtils.createTableName(p, blankNodeStrategy)
            val id = SQLUtils.escapeTablename(tableName + lang.getOrElse(""))
            objectType match {
              case 0 | 1 =>
                println(s"p:$p")
                createMapping(id, tableName, p)
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
