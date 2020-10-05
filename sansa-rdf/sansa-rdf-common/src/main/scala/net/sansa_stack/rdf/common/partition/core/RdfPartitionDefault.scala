package net.sansa_stack.rdf.common.partition.core

import net.sansa_stack.rdf.common.partition.layout.TripleLayout
import org.apache.jena.graph.Triple

/**
 * special datatypes: b for blank, u for uri, typed literal otherwise
 */
case class RdfPartitionDefault(subjectType: Byte,
                               predicate: String,
                               objectType: Byte,
                               datatype: String,
                               langTagPresent: Boolean)
  extends RdfPartition
    with Serializable {
  def layout: TripleLayout = RdfPartitionerDefault.determineLayout(this)

  def matches(t: Triple): Boolean = {
    val p = RdfPartitionerDefault.fromTriple(t)
    p == this
  }
}
