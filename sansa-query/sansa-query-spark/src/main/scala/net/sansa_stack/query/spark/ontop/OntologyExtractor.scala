package net.sansa_stack.query.spark.ontop

import com.github.owlcs.ontapi.OntManagers.OWLAPIImplProfile
import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils
import net.sansa_stack.rdf.common.partition.utils.SQLUtils
import net.sansa_stack.rdf.spark.utils.ScalaUtils
import org.aksw.r2rml.jena.vocab.RR
import org.aksw.sparqlify.core.sql.common.serialization.SqlEscaperBacktick
import org.apache.jena.rdf.model.Model
import org.apache.jena.vocabulary.RDF
import org.apache.spark.sql.SparkSession
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.{IRI, OWLAxiom, OWLOntology}

import scala.jdk.CollectionConverters._

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
  def extract(spark: SparkSession, mappingsModel: Model): Option[OWLOntology] = {
    logger.debug("extracting ontology from dataset")

    // get the rdf:type TripleMaps with o being an IRI
    val tms = R2rmlUtils.triplesMapsForPredicate(RDF.`type`, mappingsModel)
      .filter(_.getPredicateObjectMaps.asScala.exists(_.getObjectMaps.asScala.exists(_.asTermMap().getTermType == RR.IRI.inModel(mappingsModel))))

    val clsCol = sqlEscaper.escapeAliasName("cls")

    val sql =
      tms.map(tm => {
        val tableName = tm.getLogicalTable.asBaseTableOrView().getTableName

        val o = tm.getPredicateObjectMaps.asScala.head.getObjectMaps.asScala.head.asTermMap().getColumn // TODO we assume a single predicate-object map here

        // we have to unwrap the quote from H2 escape and also apply Spark SQL escape
        val tn = SQLUtils.parseTableIdentifier(tableName)
        val to = sqlEscaper.escapeColumnName(ScalaUtils.unQuote(o))

        s"SELECT DISTINCT $to AS $clsCol FROM $tn"

      }).mkString(" UNION ")

    if (sql.nonEmpty) {
      println(s"class retrieval query:\n$sql")
      val df = spark.sql(sql)

      val classes = df.collect().map(_.getString(0))

      // we just use declaration axioms for now
      val dataFactory = OWLManager.getOWLDataFactory
      val axioms: Set[OWLAxiom] = classes.map(cls =>
        dataFactory.getOWLDeclarationAxiom(dataFactory.getOWLClass(IRI.create(cls)))).toSet
      val ontology = createOntology(axioms)

      import org.semanticweb.owlapi.formats.RDFXMLDocumentFormat

      import java.io.{File, FileOutputStream}
      ontology.saveOntology(new RDFXMLDocumentFormat(), new FileOutputStream(new File("/tmp/ontop-ontology.rdf")))

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
