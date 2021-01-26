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

  // create the tmp DB needed for Ontop
  val JDBC_URL = "jdbc:h2:mem:sansaontopdb;DATABASE_TO_UPPER=FALSE"
//  val JDBC_URL = "jdbc:h2:file:/tmp/sansaontopdb;DATABASE_TO_UPPER=FALSE"
  val JDBC_USER = "sa"
  val JDBC_PASSWORD = ""

  lazy val connection: Connection = try {
    logger.debug("creating DB connection ...")
    val conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)
    logger.debug(" ... done")
    conn
  } catch {
    case e: SQLException =>
      throw e
  }
  sys.addShutdownHook {
    connection.close()
  }

  var configs = Map[String, OntopReformulationSQLConfiguration]()

  def apply(id: String,
            obdaMappings: Model,
            properties: Properties,
            jdbcMetaData: Map[String, String],
            ontology: Option[OWLOntology]): OntopReformulationSQLConfiguration = {

    val conf = configs.getOrElse(id, {
      logger.debug("creating reformulation config ...")
      println("creating reformulation config ...")
      val reformulationConfiguration = {
        JDBCDatabaseGenerator.generateTables(connection, jdbcMetaData)

        OntopUtils.createReformulationConfig(obdaMappings, properties, ontology)
      }

      configs += id -> reformulationConfiguration

      logger.debug("...done")
      reformulationConfiguration
    })
    conf
  }

  def clear(): Unit = configs = Map[String, OntopReformulationSQLConfiguration]()

}
