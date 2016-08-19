package org.sansa.inference.utils.graph

import java.util.Comparator

import com.google.common.collect.ComparisonChain
import org.apache.jena.graph.Node

/**
  * Definition of edge equivalence used for graph isomorphism detection.
  *
  * @author Lorenz Buehmann
  */
class EdgeEquivalenceComparator extends Comparator[LabeledEdge[Node, String]] {

//  def equivalenceHashcode(edge: LabeledEdge[Node], context: org.jgrapht.Graph[Node, LabeledEdge[Node]]): Int = edge.hashCode()
//
//  def equivalenceCompare(edge1: LabeledEdge[Node], edge2: LabeledEdge[Node],
//                                  context1: org.jgrapht.Graph[Node, LabeledEdge[Node]], context2: org.jgrapht.Graph[Node, LabeledEdge[Node]]): Boolean =
//    (edge1.label.startsWith("?") && edge2.label.startsWith("?")) || // both predicates are variables
//     edge1.label.equals(edge2.label) // both URIs match
  override def compare(e1: LabeledEdge[Node, String], e2:
   LabeledEdge[Node, String]): Int =
  if((e1.label.startsWith("?") && e2.label.startsWith("?")) || e1.label.equals(e2.label))
    0
  else {
    ComparisonChain.start().compare(e1.label, e2.label).result()
  }
}
