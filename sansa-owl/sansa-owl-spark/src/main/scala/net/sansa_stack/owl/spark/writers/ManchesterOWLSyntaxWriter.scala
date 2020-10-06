package net.sansa_stack.owl.spark.writers

import java.io.{ByteArrayOutputStream, OutputStreamWriter}
import java.util.Collections

import scala.collection.JavaConverters._

import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.formats.ManchesterSyntaxDocumentFormat
import org.semanticweb.owlapi.manchestersyntax.renderer.{ManchesterOWLSyntaxFrameRenderer, ManchesterOWLSyntaxPrefixNameShortFormProvider}
import org.semanticweb.owlapi.model.OWLOntology

import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

protected class SANSAManchesterOWLSyntaxFrameRenderer(
                                                       ont: OWLOntology,
                                                       writer: OutputStreamWriter,
                                                       prefixNameShortFormProvider: ManchesterOWLSyntaxPrefixNameShortFormProvider)
  extends ManchesterOWLSyntaxFrameRenderer(ont, writer, prefixNameShortFormProvider) {

  override def writeOntologyHeader(): Unit = None
  override def writePrefixMap(): Unit = None
}

object ManchesterOWLSyntaxWriter extends OWLWriterBase {
  override def save(filePath: String, owlAxioms: OWLAxiomsRDD): Unit =
    owlAxioms.mapPartitions(partition => if (partition.hasNext) {
      val os = new ByteArrayOutputStream()
      val osWriter = new OutputStreamWriter(os)

      val man = OWLManager.createOWLOntologyManager()
      val ont = man.createOntology(partition.toStream.asJava)

      val renderer = new SANSAManchesterOWLSyntaxFrameRenderer(
        ont, osWriter,
        new ManchesterOWLSyntaxPrefixNameShortFormProvider(new ManchesterSyntaxDocumentFormat))
      renderer.writeOntology()
      os.flush()

      Collections.singleton(os.toString("UTF-8").trim).iterator().asScala

    } else {
      Iterator()

    }).saveAsTextFile(filePath)
}
