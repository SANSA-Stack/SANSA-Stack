package net.sansa_stack.rdf.partition.core

import org.apache.jena.graph.Triple
import net.sansa_stack.rdf.partition.layout.TripleLayout

trait RdfPartition {
  def layout: TripleLayout
  def matches(triple: Triple): Boolean
}