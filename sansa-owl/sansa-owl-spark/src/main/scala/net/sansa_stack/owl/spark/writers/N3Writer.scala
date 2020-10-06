package net.sansa_stack.owl.spark.writers
import java.io.{BufferedWriter, ByteArrayOutputStream, OutputStreamWriter, PrintWriter}

import scala.collection.JavaConverters._
import org.eclipse.rdf4j.rio.n3.{N3Writer => RioN3Writer}
import org.eclipse.rdf4j.rio.RDFHandlerException
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.N3DocumentFormat
import org.semanticweb.owlapi.model.{OWLAnnotationProperty, OWLAxiom, OWLClass, OWLDataProperty, OWLDatatype, OWLNamedIndividual, OWLObjectProperty, OWLOntologyWriterConfiguration, OWLRuntimeException}
import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

object N3Writer extends OWLWriterBase {
  override def save(filePath: String, owlAxioms: OWLAxiomsRDD): Unit = {
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
          new RioN3Writer(buffPrintWriter),
          new N3DocumentFormat) {
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
          // make blank node IDs unique (by appending the axiom and partition ID)
          .replaceAll("_:genid([0-9]+)", "_:genid$1" + s"_${axiomIdx}_$idx")
          .replaceAll("^\\s*#.*", "").replaceAll("\n\\s*#.*", "")
          .replaceAll("(?m)@prefix.+$", "")
          .trim + System.lineSeparator()
      })
//      Collections.singleton(os.toString("UTF-8").trim).iterator().asScala
    } else {
      Iterator()
    }).saveAsTextFile(filePath)
  }
}
