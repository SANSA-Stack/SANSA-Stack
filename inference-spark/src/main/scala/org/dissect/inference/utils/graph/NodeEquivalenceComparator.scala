package org.dissect.inference.utils.graph

import org.apache.jena.graph.Node
import org.jgrapht.Graph
import org.jgrapht.experimental.equivalence.EquivalenceComparator

/**
  * Definition of node equivalence used for graph isomorphism detection.
  *
  * @author Lorenz Buehmann
  */
class NodeEquivalenceComparator extends EquivalenceComparator[Node, Graph[Node, LabeledEdge]] {
  override def equivalenceHashcode(node: Node, context: Graph[Node, LabeledEdge]): Int = node.hashCode()

  override def equivalenceCompare(node1: Node, node2: Node,
                                  context1: Graph[Node, LabeledEdge], context2: Graph[Node, LabeledEdge]): Boolean = node1.equals(node2)
}
