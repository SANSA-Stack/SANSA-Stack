package net.sansa_stack.rdf.common.partition.core

import org.apache.jena.graph.Triple

import net.sansa_stack.rdf.common.partition.layout.TripleLayout

/**
 * special datatypes: b for blank, u for uri, typed literal otherwise
 */
case class RdfPartitionComplex(subjectType: Byte,
                               predicate: String,
                               objectType: Byte,
                               datatype: String,
                               langTagPresent: Boolean)
  extends RdfPartition
    with Serializable {
  def layout: TripleLayout = RdfPartitionerComplex.determineLayout(this)

  def matches(t: Triple): Boolean = {
    val p = RdfPartitionerComplex.fromTriple(t)
    p == this
  }
}
