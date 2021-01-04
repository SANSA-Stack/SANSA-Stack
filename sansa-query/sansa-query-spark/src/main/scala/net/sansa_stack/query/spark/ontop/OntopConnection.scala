package net.sansa_stack.query.spark.ontop

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

import it.unibz.inf.ontop.answering.connection.OntopConnection
import it.unibz.inf.ontop.injection.OntopReformulationSQLConfiguration
import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner}
import org.semanticweb.owlapi.model.OWLOntology

/**
 * Used to keep expensive resource per executor alive.
 *
 * @author Lorenz Buehmann
 */
object OntopConnection {

  val logger = com.typesafe.scalalogging.Logger(classOf[OntopConnection])

  // create the tmp DB needed for Ontop
  private val JDBC_URL = "jdbc:h2:mem:sansaontopdb;DATABASE_TO_UPPER=FALSE"
  private val JDBC_USER = "sa"
  private val JDBC_PASSWORD = ""

  lazy val connection: Connection = try {
//    println("creating DB connection ")
    val conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)
//    println("created DB connection")
    conn
  } catch {
    case e: SQLException =>
      throw e
  }
  sys.addShutdownHook {
    connection.close()
  }

  var configs = Map[Set[RdfPartitionStateDefault], OntopReformulationSQLConfiguration]()

  def apply(obdaMappings: String, properties: Properties, partitioner: RdfPartitioner[RdfPartitionStateDefault], partitions: Set[RdfPartitionStateDefault], ontology: Option[OWLOntology]): OntopReformulationSQLConfiguration = {
    val conf = configs.getOrElse(partitions, {
      logger.debug("creating reformulation config")
      val reformulationConfiguration = {
        JDBCDatabaseGenerator.generateTables(connection, partitioner, partitions)

        OntopUtils.createReformulationConfig(obdaMappings, properties, ontology)
      }

      configs += partitions -> reformulationConfiguration

//      println("done")
      reformulationConfiguration
    })
    conf
  }

}
