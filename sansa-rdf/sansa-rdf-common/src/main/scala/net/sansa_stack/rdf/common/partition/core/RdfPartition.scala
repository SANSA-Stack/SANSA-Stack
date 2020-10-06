package net.sansa_stack.rdf.common.partition.core

import net.sansa_stack.rdf.common.partition.layout.TripleLayout
import org.apache.jena.graph.Triple

trait RdfPartition {
  def layout: TripleLayout
  def matches(triple: Triple): Boolean
}
