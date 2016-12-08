package net.sansa_stack.rdf.partition.core

import org.apache.jena.graph.Triple

//import scala.reflect.runtime.universe.TypeTag

trait RdfPartitioner[P <: RdfPartition] {
  def fromTriple(triple: Triple): P
}

