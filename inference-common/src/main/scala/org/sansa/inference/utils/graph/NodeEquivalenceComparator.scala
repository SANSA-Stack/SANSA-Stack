package org.sansa.inference.utils.graph

import java.util.Comparator

import org.apache.jena.graph.Node
import org.apache.jena.sparql.util.NodeComparator

/**
  * Definition of node equivalence used for graph isomorphism detection.
  *
  * @author Lorenz Buehmann
  */
class NodeEquivalenceComparator extends Comparator[Node] {
  val c = new NodeComparator()
  override def compare(o1: Node, o2: Node): Int = c.compare(o1, o2)
}
