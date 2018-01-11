package net.sansa_stack.rdf.partition.core

import org.apache.jena.graph.Triple
import net.sansa_stack.rdf.partition.core.RdfPartition

//import scala.reflect.runtime.universe.TypeTag

trait RdfPartitioner[P <: RdfPartition] {
  def fromTriple(triple: Triple): P
}

