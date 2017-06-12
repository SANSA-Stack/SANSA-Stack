package net.sansa_stack.rdf.partition.layout

import scala.reflect.runtime.universe.Type

import org.apache.jena.graph.Triple
import org.aksw.jena_sparql_api.views.E_Triple


trait TripleLayout {
  def schema: Type
  def fromTriple(triple: Triple): Product
}
