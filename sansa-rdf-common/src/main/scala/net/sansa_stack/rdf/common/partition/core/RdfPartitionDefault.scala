package net.sansa_stack.rdf.common.partition.core

import org.apache.jena.graph.Triple
/**
 * special datatypes: b for blank, u for uri, typed literal otherwise
 */
case class RdfPartitionDefault(val subjectType: Byte, val predicate: String, val objectType: Byte, val datatype: String, val langTagPresent: Boolean)
  extends RdfPartition with Serializable
{
  def layout = RdfPartitionerDefault.determineLayout(this)

  def matches(t: Triple): Boolean = {
    val p = RdfPartitionerDefault.fromTriple(t)
    p == this
  }
}
