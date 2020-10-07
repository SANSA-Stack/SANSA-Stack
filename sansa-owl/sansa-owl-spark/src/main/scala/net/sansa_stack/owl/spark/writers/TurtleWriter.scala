package net.sansa_stack.owl.spark.writers
import java.io.{BufferedWriter, ByteArrayOutputStream, OutputStreamWriter, PrintWriter}
import java.util.Collections

import scala.collection.JavaConverters._

import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.TurtleDocumentFormat
import org.semanticweb.owlapi.io.RDFResource
import org.semanticweb.owlapi.model.{OWLAnnotationProperty, OWLClass, OWLDataProperty, OWLDatatype, OWLNamedIndividual, OWLObjectProperty, OWLOntology, OWLOntologyWriterConfiguration}
import org.semanticweb.owlapi.rdf.turtle.renderer.TurtleRenderer

import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

protected class SANSATurtleRenderer(
                                     ont: OWLOntology,
                                     writer: PrintWriter,
                                     documentFormat: TurtleDocumentFormat)
    extends TurtleRenderer(ont, writer, documentFormat) {

  override def beginDocument(): Unit = pending.clear()

  override def endDocument(): Unit = {
    writer.flush()
    pending.clear()
  }

  override def render(node: RDFResource, root: Boolean): Unit = {
    if (!node.getIRI.getNamespace.equals("urn:unnamed:ontology#")) {
      super.render(node, root)
    }
  }

  override def writeBanner(name: String): Unit = None
  override def writeClassComment(cls: OWLClass): Unit = None
  override def writeObjectPropertyComment(prop: OWLObjectProperty): Unit = None
  override def writeDataPropertyComment(prop: OWLDataProperty): Unit = None
  override def writeIndividualComments(ind: OWLNamedIndividual): Unit = None
  override def writeAnnotationPropertyComment(prop: OWLAnnotationProperty): Unit = None
  override def writeDatatypeComment(datatype: OWLDatatype): Unit = None
}

object TurtleWriter extends OWLWriterBase {
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

        val renderer =
          new SANSATurtleRenderer(ont, buffPrintWriter, new TurtleDocumentFormat)
        renderer.render()
      })

      buffPrintWriter.flush()

      Collections.singleton(os.toString("UTF-8").trim).iterator().asScala
    } else {
      Iterator()
    }).saveAsTextFile(filePath)
}
