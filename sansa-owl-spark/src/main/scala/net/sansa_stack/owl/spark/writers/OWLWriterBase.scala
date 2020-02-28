package net.sansa_stack.owl.spark.writers

import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

abstract class OWLWriter {
  def save(filePath: String, owlAxioms: OWLAxiomsRDD)
}
