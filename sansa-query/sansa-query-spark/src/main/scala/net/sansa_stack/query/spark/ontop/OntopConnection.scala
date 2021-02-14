package net.sansa_stack.query.spark.ontop

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

import it.unibz.inf.ontop.answering.connection.OntopConnection
import it.unibz.inf.ontop.injection.OntopReformulationSQLConfiguration
import org.apache.jena.rdf.model.Model
import org.semanticweb.owlapi.model.OWLOntology

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner}

/**
 * Used to keep expensive resource per executor alive.
 *
 * @author Lorenz Buehmann
 */
object OntopConnection {

  val logger = com.typesafe.scalalogging.Logger(classOf[OntopConnection])

  val DEFAULT_DATABASE = "default"

  // create the tmp DB needed for Ontop
//  val JDBC_URL = "jdbc:h2:mem:sansaontopdb;DATABASE_TO_UPPER=FALSE"
//  val JDBC_URL = "jdbc:h2:file:/tmp/sansaontopdb;DATABASE_TO_UPPER=FALSE"
  val JDBC_USER = "sa"
  val JDBC_PASSWORD = ""

  // scalastyle:off classforname
  Class.forName("org.h2.Driver")
  // scalastyle:on classforname

  // maintain multiple connections
  var connections = Map[String, Connection]()

  /**
   * Does return an existing or new JDBC connection for the given database. If no database has been given,
   * it will return a connection with no schema created and set - in H2 this means, "PUBLIC" will be used.
   *
   * @param database the optional database name
   * @return a JDBC connection
   */
  def getOrCreateConnection(database: Option[String]): Connection = {
    val db = database.getOrElse(DEFAULT_DATABASE) // for caching we have to use a default key
    connections.getOrElse(db, {
      try {
        logger.debug("creating DB connection ...")
        val conn = DriverManager.getConnection(getConnectionURL(database), JDBC_USER, JDBC_PASSWORD)
        connections += db -> conn
        sys.addShutdownHook {
          conn.close()
        }
        logger.debug(" ... done")
        conn
      } catch {
        case e: SQLException =>
          throw e
      }

    })
  }

  /**
   * Does return a JDBC connection URL. If a database has been given, it will be provided as a H2 schema which will
   * be created and activated.
   *
   * @param database the optional database name
   * @return a JDBC connection URL
   */
  def getConnectionURL(database: Option[String] = None): String = {
    var url = s"jdbc:h2:mem:ontop_sansa_db;DATABASE_TO_UPPER=FALSE;"
    if (database.isDefined) url += s"INIT=CREATE SCHEMA IF NOT EXISTS ${database.get}\\;SET SCHEMA ${database.get}"
    url
  }

  // maintain multiple reformulation configs, one per session
  var configs = Map[String, OntopReformulationSQLConfiguration]()

  def apply(id: String,
            database: Option[String],
            obdaMappings: Model,
            properties: Properties,
            jdbcMetaData: Map[String, String],
            ontology: Option[OWLOntology]): OntopReformulationSQLConfiguration = {

    val conf = configs.getOrElse(id, {
      logger.debug(s"creating reformulation config for session $id...")
      println(s"creating reformulation config for session $id...")
      val reformulationConfiguration = {
        JDBCDatabaseGenerator.generateTables(getOrCreateConnection(database), jdbcMetaData)

        OntopUtils.createReformulationConfig(database, obdaMappings, properties, ontology)
      }

      configs += id -> reformulationConfiguration

      logger.debug("...done")
      reformulationConfiguration
    })
    conf
  }

  def clear(): Unit = {
    configs = Map[String, OntopReformulationSQLConfiguration]()
    connections.foreach {case (_, conn) => conn.close()}
    connections = Map[String, Connection]()
  }

}
