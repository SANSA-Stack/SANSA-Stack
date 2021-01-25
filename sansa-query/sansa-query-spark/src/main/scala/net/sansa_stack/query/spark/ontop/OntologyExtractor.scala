package net.sansa_stack.query.spark.ontop

import com.github.owlcs.ontapi.OntManagers.OWLAPIImplProfile
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperBacktick
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{IRI, OWLAxiom, OWLOntology}

import scala.collection.JavaConverters._

/**
 * An extractor for an ontology.
 *
 * @author Lorenz Buehmann
 */
object OntologyExtractor {

  val logger = com.typesafe.scalalogging.Logger(OntologyExtractor.getClass)

  val sqlEscaper = new SqlEscaperBacktick()

  /**
   * creates an ontology from the given partitions.
   * This is necessary for rdf:type information which is handled not in forms of SQL tables by Ontop
   * but by a given set of entities contained in the ontology.
   * TODO we we just use class declaration axioms for now
   *      but it could be extended to extract more sophisticated schema axioms that can be used for inference
   */
  def extract(spark: SparkSession): Option[OWLOntology] = {
    logger.debug("extracting ontology from dataset")
    // get the partitions that contain the rdf:type triples
    val typePartitions = spark.catalog.listTables().filter(_.name.contains("type")).collect()

    if (typePartitions.nonEmpty) {
      // generate the table names for those rdf:type partitions
      // there can be more than one because the partitioner creates a separate partition for each subject and object type
      val names = typePartitions.map(_.name)

      // create the SQL query as UNION of
      val sql = names.map(name => s"SELECT DISTINCT o FROM ${sqlEscaper.escapeTableName(name)}").mkString(" UNION ")

      val df = spark.sql(sql)

      val classes = df.collect().map(_.getString(0))

      // we just use declaration axioms for now
      val dataFactory = OWLManager.getOWLDataFactory
      val axioms: Set[OWLAxiom] = classes.map(cls =>
        dataFactory.getOWLDeclarationAxiom(dataFactory.getOWLClass(IRI.create(cls)))).toSet
      val ontology = createOntology(axioms)

      logger.info(s"ontology contains ${classes.length} classes and ${ontology.getLogicalAxiomCount()} logical axioms.")

      Some(ontology)
    } else {
      None
    }
  }

  /**
   * creates a non-concurrent aware ontology - used to avoid overhead during serialization.
   */
  private def createOntology(axioms: Set[OWLAxiom]): OWLOntology = {
    val man = new OWLAPIImplProfile().createManager(false)
    man.createOntology(axioms.asJava)
  }

}
