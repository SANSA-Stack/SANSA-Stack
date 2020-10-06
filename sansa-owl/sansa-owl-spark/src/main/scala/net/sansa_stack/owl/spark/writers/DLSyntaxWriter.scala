package net.sansa_stack.owl.spark.writers

import org.semanticweb.owlapi.dlsyntax.renderer.DLSyntaxObjectRenderer

import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

object DLSyntaxWriter extends OWLWriterBase {
  override def save(filePath: String, owlAxioms: OWLAxiomsRDD): Unit =
    owlAxioms.mapPartitions(partition => if (partition.hasNext) {
      val renderer = new DLSyntaxObjectRenderer()

      partition.map(renderer.render(_))

    } else {
      Iterator()

    }).saveAsTextFile(filePath)
}
