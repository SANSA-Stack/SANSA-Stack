package net.sansa_stack.owl.spark.writers

import java.io.{BufferedWriter, ByteArrayOutputStream, OutputStreamWriter, PrintWriter}

import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD
import org.eclipse.rdf4j.rio.trix.{TriXWriter => RioTriXWriter}
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.TrixDocumentFormat
import org.semanticweb.owlapi.model._

import scala.jdk.CollectionConverters._

object TrixWriter extends OWLWriterBase {
  override def save(filePath: String, owlAxioms: OWLAxiomsRDD): Unit =
    owlAxioms.mapPartitionsWithIndex((idx: Int, partition: Iterator[OWLAxiom]) => if (partition.hasNext) {
      partition.zipWithIndex.map(axiomWIdx => {
        val axiom: OWLAxiom = axiomWIdx._1
        val axiomIdx = axiomWIdx._2

        // writer stuff
        val os = new ByteArrayOutputStream()
        val osWriter = new OutputStreamWriter(os)
        val buffPrintWriter = new PrintWriter(new BufferedWriter(osWriter))

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

        val renderer = new SANSARioRenderer(
          ont,
          new RioTriXWriter(buffPrintWriter),
          new TrixDocumentFormat) {
          override def writeBanner(name: String): Unit = None
          override def writeAnnotationPropertyComment(prop: OWLAnnotationProperty): Unit = None
          override def writeClassComment(cls: OWLClass): Unit = None
          override def writeDataPropertyComment(prop: OWLDataProperty): Unit = None
          override def writeIndividualComments(ind: OWLNamedIndividual): Unit = None
          override def writeDatatypeComment(datatype: OWLDatatype): Unit = None
          override def writeObjectPropertyComment(prop: OWLObjectProperty): Unit = None
        }

        renderer.render()
        buffPrintWriter.flush()

        os.toString("UTF-8")
          .replaceAll("(?m)<\\?xml.+$", "")
          .replaceAll("(?m)<!-.+-->$", "")
          .replaceAll("(?m)<TriX.+$", "")
          .replaceAll("(?m)</TriX.+$", "")
          .replaceAll("(?m)<graph.+$", "")
          .replaceAll("(?m)</graph.+$", "")
          .replaceAll("(?m)^\\s*$", "")
          .replaceAll("genid([0-9]+)", "genid$1" + s"_${axiomIdx}_$idx")
      })
    } else {
      Iterator()
    }).saveAsTextFile(filePath)
}
