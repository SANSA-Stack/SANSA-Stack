package net.sansa_stack.rdf.partition.layout

import scala.reflect.runtime.universe.Type

import org.apache.jena.graph.Triple


trait TripleLayout {
  def schema: Type
  def fromTriple(triple: Triple): Product
  def fromTripleToC(triple: Triple): Product
}
