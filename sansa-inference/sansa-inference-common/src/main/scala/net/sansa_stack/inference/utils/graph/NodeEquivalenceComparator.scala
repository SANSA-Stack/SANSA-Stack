package net.sansa_stack.inference.utils.graph

import org.apache.jena.graph.Node
import org.apache.jena.sparql.util.NodeCmp

import java.util.Comparator

/**
  * Definition of node equivalence used for graph isomorphism detection.
  *
  * @author Lorenz Buehmann
  */
class NodeEquivalenceComparator extends Comparator[Node] {
  val c : Comparator[Node] = NodeCmp.compareRDFTerms;
  override def compare(o1: Node, o2: Node): Int = c.compare(o1, o2)
}
