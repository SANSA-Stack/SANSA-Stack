package net.sansa_stack.owl.spark.writers

import java.io.{ByteArrayOutputStream, OutputStreamWriter}
import java.util.Collections

import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.krss2.renderer.KRSS2OWLObjectRenderer

import scala.jdk.CollectionConverters._

object KRSS2SyntaxWriter extends OWLWriterBase {
  override def save(filePath: String, owlAxioms: OWLAxiomsRDD): Unit =
    owlAxioms.mapPartitions(partition => if (partition.hasNext) {
      val os = new ByteArrayOutputStream()
      val osWriter = new OutputStreamWriter(os)
      val renderer = new KRSS2OWLObjectRenderer(OWLManager.createOWLOntologyManager().createOntology(), osWriter)

      partition.foreach(axiom => {
        val ont = OWLManager.createOWLOntologyManager().createOntology(Seq(axiom).asJava)
        renderer.visit(ont)
      })
      osWriter.flush()

      Collections.singleton(os.toString("UTF-8").trim + nl).iterator().asScala

    } else {
      Iterator()

    }).saveAsTextFile(filePath)
}
