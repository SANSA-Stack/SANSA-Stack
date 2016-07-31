package org.dissect.inference.utils.graph

import org.apache.jena.graph.Node
import org.jgrapht.experimental.equivalence.EquivalenceComparator

/**
  * Definition of edge equivalence used for graph isomorphism detection.
  *
  * @author Lorenz Buehmann
  */
class EdgeEquivalenceComparator extends EquivalenceComparator[LabeledEdge, org.jgrapht.Graph[Node, LabeledEdge]] {
  override def equivalenceHashcode(edge: LabeledEdge, context: org.jgrapht.Graph[Node, LabeledEdge]): Int = edge.hashCode()

  override def equivalenceCompare(edge1: LabeledEdge, edge2: LabeledEdge,
                                  context1: org.jgrapht.Graph[Node, LabeledEdge], context2: org.jgrapht.Graph[Node, LabeledEdge]): Boolean =
    (edge1.label.startsWith("?") && edge2.label.startsWith("?")) || // both predicates are variables
     edge1.label.equals(edge2.label) // both URIs match
}
