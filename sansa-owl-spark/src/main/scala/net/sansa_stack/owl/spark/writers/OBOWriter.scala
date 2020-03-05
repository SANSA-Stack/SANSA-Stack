package net.sansa_stack.owl.spark.writers

import java.io.{BufferedWriter, ByteArrayOutputStream, OutputStreamWriter, PrintWriter}
import java.util.Collections

import scala.collection.JavaConverters._

import org.obolibrary.obo2owl.OWLAPIOwl2Obo
import org.obolibrary.oboformat.writer.OBOFormatWriter
import org.semanticweb.owlapi.apibinding.OWLManager

import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

object OBOWriter extends OWLWriterBase {
  private val nl = System.getProperty("line.separator")

  override def save(filePath: String, owlAxioms: OWLAxiomsRDD): Unit =
    owlAxioms.mapPartitions(partition => if (partition.hasNext) {
      val os = new ByteArrayOutputStream()
      val osWriter = new OutputStreamWriter(os)
      val buffWriter = new PrintWriter(new BufferedWriter(osWriter))
      val translator = new OWLAPIOwl2Obo(OWLManager.createOWLOntologyManager())
      val oboWriter = new OBOFormatWriter

      partition.foreach(axiom => {
        val ont = OWLManager.createOWLOntologyManager().createOntology(Seq(axiom).asJava)
        val translation = translator.convert(ont)
        val nameProvider = new OBOFormatWriter.OBODocNameProvider(translation)

        translation.getTypedefFrames.asScala.foreach(oboWriter.write(_, buffWriter, nameProvider))
        translation.getTermFrames.asScala.foreach(oboWriter.write(_, buffWriter, nameProvider))
        translation.getInstanceFrames.asScala.foreach(oboWriter.write(_, buffWriter, nameProvider))
      })
      buffWriter.flush()

      Collections.singleton(os.toString("UTF-8").trim + nl).iterator().asScala

    } else {
      Iterator()
    }).saveAsTextFile(filePath)
}
