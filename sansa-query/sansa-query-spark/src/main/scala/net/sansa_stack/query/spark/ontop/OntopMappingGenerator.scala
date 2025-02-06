package net.sansa_stack.query.spark.ontop

import net.sansa_stack.rdf.common.partition.core.RdfPartitionStateDefault
import net.sansa_stack.rdf.common.partition.utils.SQLUtils
import net.sansa_stack.rdf.spark.partition.core.BlankNodeStrategy
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperDoubleQuote
import org.apache.jena.vocabulary.{RDF, XSD}
import org.semanticweb.owlapi.model.OWLOntology
import org.semanticweb.owlapi.model.parameters.Imports

import scala.jdk.CollectionConverters._

/**
 * @author Lorenz Buehmann
 */
object OntopMappingGenerator {

  val logger = com.typesafe.scalalogging.Logger(OntopMappingGenerator.getClass)

  val sqlEscaper = new SqlEscaperDoubleQuote()

  val blankNodeStrategy: BlankNodeStrategy.Value = BlankNodeStrategy.Table
  val distinguishStringLiterals: Boolean = false

  def createOBDAMappingsForPartitions(partitions: Set[RdfPartitionStateDefault], ontology: Option[OWLOntology] = None): String = {

    // object is URI or bnode
    def createMapping(id: String, tableName: String, partition: RdfPartitionStateDefault): String = {

      val targetSubject = if (partition.subjectType == 0) "_:{s}" else "<{s}>"
      val targetObject = if (partition.objectType == 0) "_:{o}" else "<{o}>"

      if (blankNodeStrategy == BlankNodeStrategy.Table) {
        s"""
           |mappingId     $id
           |source        SELECT "s", "o" FROM ${sqlEscaper.escapeTableName(tableName)}
           |target        $targetSubject <${partition.predicate}> $targetObject .
           |""".stripMargin
      } else if (blankNodeStrategy == BlankNodeStrategy.Column) {
        val whereCondition = s"${if (partition.subjectType == 1) "NOT" else ""} s_blank AND" +
          s" ${if (partition.objectType == 1) "NOT" else ""} o_blank"

        s"""
           |mappingId     $id
           |source        SELECT "s", "o" FROM ${sqlEscaper.escapeTableName(tableName)} WHERE $whereCondition
           |target        $targetSubject <${partition.predicate}> $targetObject .
           |""".stripMargin
      } else {
        throw new RuntimeException(s"Unsupported Blank Node Strategy:$blankNodeStrategy")
      }
    }

    // object is string literal
    def createMappingStringLit(id: String, tableName: String, partition: RdfPartitionStateDefault): String = {
      val languages = partition.languages
      val targetSubject = if (partition.subjectType == 0) "_:{s}" else "<{s}>"

      if (languages.isEmpty) { // no language tag, i.e. xsd:string
        s"""
           |mappingId     $id
           |source        SELECT "s", "o" FROM ${sqlEscaper.escapeTableName(tableName)} WHERE "l" = ''
           |target        $targetSubject <${partition.predicate}> "{o}" .
           |""".stripMargin
      } else {
        languages.map(lang => {
          // need a local ID per language
          val id = tableName + "_lang"
          s"""
             |mappingId     $id
             |source        SELECT "s", "o" FROM ${sqlEscaper.escapeTableName(tableName)} WHERE "l" = '$lang'
             |target        $targetSubject <${partition.predicate}> "{o}"@$lang .
             |""".stripMargin
        }).mkString("\n")
      }
    }

    // object is other literal
    def createMappingLit(id: String, tableName: String, partition: RdfPartitionStateDefault): String = {
      val targetSubject = if (partition.subjectType == 0) "_:{s}" else "<{s}>"
      s"""
         |mappingId     $id
         |source        SELECT "s", "o" FROM ${sqlEscaper.escapeTableName(tableName)}
         |target        $targetSubject <${partition.predicate}> "{o}"^^<${partition.datatype}> .
         |""".stripMargin
    }

    // class assertion mapping
    def createClassMapping(id: String, tableName: String, partition: RdfPartitionStateDefault, cls: String): String = {
      val targetSubject = if (partition.subjectType == 0) "_:{s}" else "<{s}>"
      s"""
         |mappingId     $id
         |source        SELECT "s", "o" FROM ${sqlEscaper.escapeTableName(tableName)} WHERE "o" = '$cls'
         |target        $targetSubject <${RDF.`type`.getURI}> <$cls> .
         |""".stripMargin
    }

    def createMappingLiteralWithType(id: String, tableName: String, property: String): String = {
      s"""
         |mappingId     $id
         |source        SELECT "s", "o", "t" FROM ${sqlEscaper.escapeTableName(tableName)}
         |target        <{s}> <$property> "{o}"^^<{t}> .
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
          case p@RdfPartitionStateDefault(subjectType, predicate, objectType, datatype, langTagPresent, lang) =>
            val tableName = SQLUtils.createDefaultTableName(p)
            val id = sqlEscaper.escapeTableName(tableName)

            if (predicate == RDF.`type`.getURI) { // rdf:type mapping expansion here
              if (ontology.nonEmpty) ontology.get.getClassesInSignature(Imports.EXCLUDED).asScala.map(cls => createClassMapping(id, tableName, p, cls.toStringID)).mkString("\n") else ""
            } else {
              objectType match {
                case 0 | 1 => createMapping(id, tableName, p) // o is URI or bnode
                case 2 => if (distinguishStringLiterals) {
                  if (langTagPresent) createMappingStringLit(id, tableName, p) else createMappingLit(id, tableName, p)
                } else {
                  if (langTagPresent || datatype == XSD.xstring.getURI) createMappingStringLit(id, tableName, p) else createMappingLit(id, tableName, p)
                }
                case _ =>
                  logger.error("TODO: bnode Ontop mapping creation")
                  ""
              }
            }
        }
        .mkString("\n\n") + "\n]]"
  }

}
