package net.sansa_stack.owl.spark.writers

import net.sansa_stack.owl.spark.rdd.OWLAxiomsRDD

abstract class OWLWriterBase {
  protected val nl = System.getProperty("line.separator")

  def save(filePath: String, owlAxioms: OWLAxiomsRDD)
}
