package org.sansa.inference.utils.graph

import org.apache.jena.graph.Node
import org.jgrapht.experimental.equivalence.EquivalenceComparator

/**
  * Definition of edge equivalence used for graph isomorphism detection.
  *
  * @author Lorenz Buehmann
  */
class EdgeEquivalenceComparator extends EquivalenceComparator[LabeledEdge[Node], org.jgrapht.Graph[Node, LabeledEdge[Node]]] {
  override def equivalenceHashcode(edge: LabeledEdge[Node], context: org.jgrapht.Graph[Node, LabeledEdge[Node]]): Int = edge.hashCode()

  override def equivalenceCompare(edge1: LabeledEdge[Node], edge2: LabeledEdge[Node],
                                  context1: org.jgrapht.Graph[Node, LabeledEdge[Node]], context2: org.jgrapht.Graph[Node, LabeledEdge[Node]]): Boolean =
    (edge1.label.startsWith("?") && edge2.label.startsWith("?")) || // both predicates are variables
     edge1.label.equals(edge2.label) // both URIs match
}
