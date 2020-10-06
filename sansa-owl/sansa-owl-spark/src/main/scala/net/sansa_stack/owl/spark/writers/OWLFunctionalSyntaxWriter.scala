package net.sansa_stack.owl.spark.writers

import java.io.{ByteArrayOutputStream, OutputStreamWriter}
import java.util.Collections

import scala.collection.JavaConverters._

import org.semanticweb.owlapi.functional.renderer.FunctionalSyntaxObjectRenderer
import org.semanticweb.owlapi.vocab.OWLXMLVocabulary

import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

object OWLFunctionalSyntaxWriter extends OWLWriterBase {
  override def save(filePath: String, owlAxioms: OWLAxiomsRDD): Unit =
    owlAxioms.mapPartitions(partition => if (partition.hasNext) {
      val os = new ByteArrayOutputStream()
      val osWriter = new OutputStreamWriter(os)
      val renderer = new FunctionalSyntaxObjectRenderer(null, osWriter)
      renderer.renderAxioms(partition.toSeq.asJava)

      Collections.singleton(
        os.toString("UTF-8")
          .replaceAll(OWLXMLVocabulary.ONTOLOGY.getShortForm + "\\(", "")
          .replaceAll(nl + "\\)", "")
          .trim).iterator().asScala

    } else {
      Iterator()

    }).saveAsTextFile(filePath)
}
