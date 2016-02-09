package org.dissect.rdf.spark.model

/**
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
trait RDF {
  // types related to the RDF data model
  type Graph
  type Triple
  type Node
  type URI <: Node
  type BNode <: Node
  type Literal <: Node
  type Lang

  // types for the graph traversal API
  type NodeMatch
  type NodeAny <: NodeMatch
}
