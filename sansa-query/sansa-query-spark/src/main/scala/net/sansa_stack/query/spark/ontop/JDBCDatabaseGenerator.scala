package net.sansa_stack.query.spark.ontop

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner}
import net.sansa_stack.rdf.common.partition.schema._
import net.sansa_stack.rdf.common.partition.utils.SQLUtils
import net.sansa_stack.rdf.spark.partition.core.BlankNodeStrategy
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperDoubleQuote
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import java.sql.{Connection, SQLException}
import scala.reflect.runtime.universe.typeOf

/**
 * Setup the JDBC database needed for the Ontop metadata extraction.
 *
 * @author Lorenz Buehmann
 */
object JDBCDatabaseGenerator {

  val logger = com.typesafe.scalalogging.Logger(JDBCDatabaseGenerator.getClass)

  val sqlEscaper = new SqlEscaperDoubleQuote()

  // mapping from partition type to H2 database type
  private val partitionType2DatabaseType = Map(
    typeOf[SchemaStringLong] -> "LONG",
    typeOf[SchemaStringDouble] -> "DOUBLE",
    typeOf[SchemaStringFloat] -> "FLOAT",
    typeOf[SchemaStringDecimal] -> "DECIMAL",
    typeOf[SchemaStringBoolean] -> "BOOLEAN",
    typeOf[SchemaStringString] -> "VARCHAR(255)",
    typeOf[SchemaStringDate] -> "DATE",
    typeOf[SchemaStringTimestamp] -> "TIMESTAMP"
  ) // .map(e => (typeOf[e._1.type], e._2))

  private val spark2H2DatatypeMapping = Map(
    "STRING" -> "VARCHAR"
  )

  def generateJdbcCommand(jdbcMetaData: Map[String, String]): String = {
    jdbcMetaData.map { case (tableName, ddl) =>

      // replace Spark datatypes with H2 types
      var ddlH2 = ddl
      spark2H2DatatypeMapping.foreach{ case (from, to) => ddlH2 = ddlH2.replace(from, to) }
      ddlH2 = ddlH2.replace("`", "\"")

      s"""
         |CREATE TABLE IF NOT EXISTS ${sqlEscaper.escapeColumnName(tableName)}
         |($ddlH2)
         |""".stripMargin

    }.mkString(";")
  }

  def generateTables(connection: Connection,
                     jdbcMetaData: Map[String, String]): Unit = {

    val s = generateJdbcCommand(jdbcMetaData)
//    println(s)

    try {
      val stmt = connection.createStatement()

//      stmt.executeUpdate("DROP ALL OBJECTS")

      stmt.executeUpdate(s)

      connection.commit()
    } catch {
      case e: SQLException => logger.error("Error occurred when creating in-memory H2 database", e)
    }

  }

  /**
   * Generates the tables per partitions for the database at the given connection.
   *
   * @param connection the database connection
   * @param partitions the partitions
   */
  def generateTables(connection: Connection,
                     partitioner: RdfPartitioner[RdfPartitionStateDefault],
                     partitions: Set[RdfPartitionStateDefault],
                     blankNodeStrategy: BlankNodeStrategy.Value = BlankNodeStrategy.Table): Unit = {
    try {
      val stmt = connection.createStatement()

      stmt.executeUpdate("DROP ALL OBJECTS")

      partitions.foreach { p =>

        val name = SQLUtils.createDefaultTableName(p)

//        val sparkSchema = ScalaReflection.schemaFor(p.layout.schema).dataType.asInstanceOf[StructType]
        val schema = partitioner.determineLayout(p).schema
        val sparkSchema = ScalaReflection.schemaFor(schema).dataType.asInstanceOf[StructType]
        logger.trace(s"creating table for property ${p.predicate} with Spark schema $sparkSchema and layout ${schema}")

        p match {
          case RdfPartitionStateDefault(subjectType, predicate, objectType, datatype, langTagPresent, lang) =>
            val s = objectType match {
              case 0|1 => s"CREATE TABLE IF NOT EXISTS ${sqlEscaper.escapeTableName(name)} (" +
                "s varchar(255) NOT NULL," +
                "o varchar(255) NOT NULL" +
                ")"
              case 2 => if (schema == typeOf[SchemaStringStringLang]) {
                s"CREATE TABLE IF NOT EXISTS ${sqlEscaper.escapeTableName(name)} (" +
                  "s varchar(255) NOT NULL," +
                  "o varchar(255) NOT NULL," +
                  "l varchar(10)" +
                  ")"
              } else {
                if (schema == typeOf[SchemaStringStringType]) {
                  s"CREATE TABLE IF NOT EXISTS ${sqlEscaper.escapeTableName(name)} (" +
                    "s varchar(255) NOT NULL," +
                    "o varchar(255) NOT NULL," +
                    "t varchar(255) NOT NULL" +
                    ")"
                } else {
                  val colType = partitionType2DatabaseType.get(schema)

                  if (colType.isDefined) {
                    s"""
                       |CREATE TABLE IF NOT EXISTS ${sqlEscaper.escapeTableName(name)} (
                       |s varchar(255) NOT NULL,
                       |o ${colType.get} NOT NULL)
                       |""".stripMargin
                  } else {
                    logger.error(s"Error: couldn't create H2 table for property $predicate with schema $schema")
                    ""
                  }
                }
              }
              case _ => logger.error("TODO: bnode H2 SQL table for Ontop mappings")
                ""
            }
            logger.debug(s)
            stmt.addBatch(s)
          case _ => logger.error(s"wrong partition type: ${p}")
        }
      }
      //            stmt.addBatch(s"CREATE TABLE IF NOT EXISTS triples (" +
      //              "s varchar(255) NOT NULL," +
      //              "p varchar(255) NOT NULL," +
      //              "o varchar(255) NOT NULL" +
      //              ")")
      val numTables = stmt.executeBatch().length
      logger.debug(s"created $numTables tables")
    } catch {
      case e: SQLException => logger.error("Error occurred when creating in-memory H2 database", e)
    }
    connection.commit()
  }

}
