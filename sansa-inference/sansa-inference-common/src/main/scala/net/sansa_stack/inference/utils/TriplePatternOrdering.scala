package net.sansa_stack.inference.utils

import org.apache.jena.graph.Node
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.sparql.util.NodeCmp

import java.util.Comparator

/**
  * Ordering for triple patterns.
  *
  * @author Lorenz Buehmann
  */
class TriplePatternOrdering extends Ordering[TriplePattern]{
  implicit val comp: Comparator[Node] = NodeCmp.compareRDFTerms;

  override def compare(x: TriplePattern, y: TriplePattern): Int = {
    Ordering.by{t: TriplePattern => (t.getSubject, t.getPredicate, t.getObject)}.compare(x, y)
  }
}
