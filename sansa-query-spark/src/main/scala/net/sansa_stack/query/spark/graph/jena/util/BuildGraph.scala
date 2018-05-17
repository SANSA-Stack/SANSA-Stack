package net.sansa_stack.query.spark.graph.jena.util

import org.apache.jena.graph.Node
import org.apache.spark.graphx.Graph

/**
  * Returns a single result RDF graph containing RDF data from the input solution mapping.
  */
object BuildGraph {

  //def construct()

  def describe(mapping: Array[Map[Node, Node]], graph: Graph[Node, Node]): Graph[Node, Node] = {
    val attrSet = mapping.flatMap(m => m.valuesIterator)
    val validGraph = graph.subgraph(epred = edge => attrSet.contains(edge.srcAttr) || attrSet.contains(edge.dstAttr))
    validGraph
  }
}
