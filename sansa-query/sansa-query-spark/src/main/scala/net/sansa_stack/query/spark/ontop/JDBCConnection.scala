package net.sansa_stack.query.spark.ontop

import java.sql.{Connection, DriverManager, SQLException}

/**
 * Creates an in-memory H2 JDBC connection.
 *
 * @author Lorenz Buehmann
 */
object JDBCConnection {

  val logger = com.typesafe.scalalogging.Logger(JDBCConnection.getClass)

  val JDBC_URL: String = "jdbc:h2:mem:sansaontopdb;DATABASE_TO_UPPER=FALSE"
  val JDBC_USER: String = "sa"
  val JDBC_PASSWORD: String = ""

  var initialized: Boolean = false

  lazy val connection: Connection = try {
    // scalastyle:off classforname
    Class.forName("org.h2.Driver")
    // scalastyle:on classforname
    val conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)
    initialized = true
    conn
  } catch {
    case e: SQLException =>
      logger.error("Error occurred when creating in-memory H2 database", e)
      throw e
  }

  def close(): Unit = if (initialized) connection.close()

}
