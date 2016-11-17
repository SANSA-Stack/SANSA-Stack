package net.sansa_stack.rdf.common.partition.core

import net.sansa_stack.rdf.common.partition.layout.TripleLayout

import org.apache.jena.graph.Triple


trait RdfPartitioner[P] {
  //def determineLayout(triple: Triple): TripleLayout
  def fromTriple(triple: Triple): P
  def determineLayout(partition: P): TripleLayout
}

