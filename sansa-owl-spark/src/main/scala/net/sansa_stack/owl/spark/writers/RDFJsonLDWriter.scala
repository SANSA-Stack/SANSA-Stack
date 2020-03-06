package net.sansa_stack.owl.spark.writers

import java.io.{BufferedWriter, ByteArrayOutputStream, OutputStreamWriter, PrintWriter}
import java.util.Collections

import scala.collection.JavaConverters._

import org.eclipse.rdf4j.rio.jsonld.JSONLDWriter
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.RDFJsonLDDocumentFormat
import org.semanticweb.owlapi.model.{IRI, OWLAxiom}

import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD


object RDFJsonLDWriter extends OWLWriterBase {
  override def save(filePath: String, owlAxioms: OWLAxiomsRDD): Unit = {
    owlAxioms.mapPartitionsWithIndex((idx: Int, partition: Iterator[OWLAxiom]) => if (partition.hasNext) {
      val snippets = partition.map(axiom => {
        // writer stuff...
        val os = new ByteArrayOutputStream()
        val osWriter = new OutputStreamWriter(os)
        val buffPrintWriter = new PrintWriter(new BufferedWriter(osWriter))

        val man = OWLManager.createOWLOntologyManager()
        val ont = man.createOntology(Seq(axiom).asJava)

        val renderer = new SANSARioRenderer(
          ont,
          new JSONLDWriter(os),
          new RDFJsonLDDocumentFormat)

        renderer.render()
        buffPrintWriter.flush()

        os.toString("UTF-8")
          .replaceFirst("\\[", "")  // remove per-partition opening brackets
          .reverse.replaceFirst("\\]", "").reverse  // remove per-partition closing brackets
          // make blank node IDs unique (by appending the partition ID)
          .replaceAll("_:genid([0-9]+)", "_:genid$1" + s"_$idx")
          .replaceAll("\\s+$", "")  // trim end
      })
      Collections.singleton(snippets.mkString(",")).iterator().asScala

    } else {
      Iterator()
    }).saveAsTextFile(filePath)
  }
}
