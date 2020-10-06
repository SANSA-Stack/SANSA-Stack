package net.sansa_stack.owl.spark.writers

import java.io.{BufferedWriter, ByteArrayOutputStream, OutputStreamWriter, PrintWriter}
import java.util.Collections

import scala.collection.JavaConverters._

import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.RDFXMLDocumentFormat
import org.semanticweb.owlapi.model.{OWLAnnotationProperty, OWLClass, OWLDataProperty, OWLDatatype, OWLDocumentFormat, OWLNamedIndividual, OWLObjectProperty, OWLOntology, OWLOntologyWriterConfiguration}
import org.semanticweb.owlapi.rdf.rdfxml.renderer.RDFXMLRenderer

import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

protected class SANSARDFXMLRenderer(
                                     ont: OWLOntology,
                                     writer: PrintWriter,
                                     docFormat: OWLDocumentFormat)
    extends RDFXMLRenderer(ont, writer, docFormat) {

  override def beginDocument(): Unit = pending.clear()
  override def endDocument(): Unit = pending.clear()
  override def writeBanner(name: String): Unit = None
  override def writeAnnotationPropertyComment(prop: OWLAnnotationProperty): Unit = None
  override def writeClassComment(cls: OWLClass): Unit = None
  override def writeDataPropertyComment(prop: OWLDataProperty): Unit = None
  override def writeIndividualComments(ind: OWLNamedIndividual): Unit = None
  override def writeDatatypeComment(datatype: OWLDatatype): Unit = None
  override def writeObjectPropertyComment(prop: OWLObjectProperty): Unit = None
  override def renderOntologyHeader(): Unit = None
}

object RDFXMLWriter extends OWLWriterBase {
  override def save(filePath: String, owlAxioms: OWLAxiomsRDD): Unit =

    owlAxioms.mapPartitions(partition => if (partition.hasNext) {
      val os = new ByteArrayOutputStream()
      val osWriter = new OutputStreamWriter(os)
      val buffPrintWriter = new PrintWriter(new BufferedWriter(osWriter))

      partition.foreach(axiom => {
        val man = OWLManager.createOWLOntologyManager()
        val config = new OWLOntologyWriterConfiguration
        config.withBannersEnabled(false)
        config.withIndenting(false)
        config.withLabelsAsBanner(false)
        config.withUseNamespaceEntities(false)
        man.setOntologyWriterConfiguration(config)

        assert(man.getOntologyWriterConfiguration == config)
        val ont = man.createOntology(Seq(axiom).asJava)
        assert(ont.getOWLOntologyManager.getOntologyWriterConfiguration == config)

        val renderer = new SANSARDFXMLRenderer(
          ont,
          buffPrintWriter,
          new RDFXMLDocumentFormat)
        renderer.render()
      })
      buffPrintWriter.flush()

      Collections.singleton(os.toString("UTF-8").trim).iterator().asScala

    } else {
      Iterator()
    }).saveAsTextFile(filePath)
}

