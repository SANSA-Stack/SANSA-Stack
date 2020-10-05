package net.sansa_stack.rdf.common.partition.core

import org.apache.jena.graph.Triple

trait RdfPartitioner[P <: RdfPartition] {
  def fromTriple(triple: Triple): P
}

