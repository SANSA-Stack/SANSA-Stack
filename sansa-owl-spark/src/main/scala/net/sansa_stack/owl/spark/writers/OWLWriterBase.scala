package net.sansa_stack.owl.spark.writers

import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

abstract class OWLWriterBase {
  def save(filePath: String, owlAxioms: OWLAxiomsRDD)
}
