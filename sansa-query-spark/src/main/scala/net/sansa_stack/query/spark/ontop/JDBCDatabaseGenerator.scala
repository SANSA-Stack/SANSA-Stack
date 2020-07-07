package net.sansa_stack.query.spark.ontop

import java.sql.{Connection, SQLException}

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import net.sansa_stack.rdf.common.partition.core.RdfPartitionComplex
import net.sansa_stack.rdf.common.partition.schema.{SchemaStringBoolean, SchemaStringDate, SchemaStringDecimal, SchemaStringDouble, SchemaStringFloat, SchemaStringLong, SchemaStringString, SchemaStringStringLang, SchemaStringStringType}
import scala.reflect.runtime.universe.typeOf

/**
 * Setup the JDBC database needed for the Ontop metadata extraction.
 *
 * @author Lorenz Buehmann
 */
object JDBCDatabaseGenerator {

  val logger = com.typesafe.scalalogging.Logger(JDBCDatabaseGenerator.getClass)


  // mapping from partition type to H2 database type
  private val partitionType2DatabaseType = Map(
    typeOf[SchemaStringLong] -> "LONG",
    typeOf[SchemaStringDouble] -> "DOUBLE",
    typeOf[SchemaStringFloat] -> "FLOAT",
    typeOf[SchemaStringDecimal] -> "DECIMAL",
    typeOf[SchemaStringBoolean] -> "BOOLEAN",
    typeOf[SchemaStringString] -> "VARCHAR(255)",
    typeOf[SchemaStringDate] -> "DATE"
  ) // .map(e => (typeOf[e._1.type], e._2))

  /**
   * Generates the tables per partitions for the database at the given connection.
   *
   * @param connection the database connection
   * @param partitions the partitions
   */
  def generateTables(connection: Connection, partitions: Set[RdfPartitionComplex],
                     blankNodeStrategy: BlankNodeStrategy.Value = BlankNodeStrategy.Table): Unit = {
    try {
      val stmt = connection.createStatement()

      stmt.executeUpdate("DROP ALL OBJECTS")

      partitions.foreach { p =>

        val name = SQLUtils.createTableName(p, blankNodeStrategy)

        val sparkSchema = ScalaReflection.schemaFor(p.layout.schema).dataType.asInstanceOf[StructType]
        logger.trace(s"creating table for property ${p.predicate} with Spark schema $sparkSchema and layout ${p.layout.schema}")
//        println(s"creating table ${SQLUtils.escapeTablename(name)} for property ${p.predicate} with Spark schema $sparkSchema and layout ${p.layout.schema}")
        p match {
          case RdfPartitionComplex(subjectType, predicate, objectType, datatype, langTagPresent, lang, partitioner) =>
            objectType match {
              case 0|1 => stmt.addBatch(s"CREATE TABLE IF NOT EXISTS ${SQLUtils.escapeTablename(name)} (" +
                "s varchar(255) NOT NULL," +
                "o varchar(255) NOT NULL" +
                ")")
              case 2 => if (p.layout.schema == typeOf[SchemaStringStringLang]) {
                stmt.addBatch(s"CREATE TABLE IF NOT EXISTS ${SQLUtils.escapeTablename(name)} (" +
                  "s varchar(255) NOT NULL," +
                  "o varchar(255) NOT NULL," +
                  "l varchar(10)" +
                  ")")
              } else {
                if (p.layout.schema == typeOf[SchemaStringStringType]) {
                  stmt.addBatch(s"CREATE TABLE IF NOT EXISTS ${SQLUtils.escapeTablename(name)} (" +
                    "s varchar(255) NOT NULL," +
                    "o varchar(255) NOT NULL," +
                    "t varchar(255) NOT NULL" +
                    ")")
                } else {
                  val colType = partitionType2DatabaseType.get(p.layout.schema)

                  if (colType.isDefined) {
                    stmt.addBatch(
                      s"""
                         |CREATE TABLE IF NOT EXISTS ${SQLUtils.escapeTablename(name)} (
                         |s varchar(255) NOT NULL,
                         |o ${colType.get} NOT NULL)
                         |""".stripMargin)
                  } else {
                    logger.error(s"Error: couldn't create H2 table for property $predicate with schema ${p.layout.schema}")
                  }
                }
              }
              case _ => logger.error("TODO: bnode H2 SQL table for Ontop mappings")
            }
          case _ => logger.error("wrong partition type")
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
