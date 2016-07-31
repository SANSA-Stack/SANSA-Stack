package org.dissect.inference.utils

import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.sparql.util.NodeComparator

/**
  * Ordering for triple patterns.
  *
  * @author Lorenz Buehmann
  */
class TriplePatternOrdering extends Ordering[TriplePattern]{
  implicit val comp = new NodeComparator

  override def compare(x: TriplePattern, y: TriplePattern): Int = {
    Ordering.by{t: TriplePattern => (t.getSubject, t.getPredicate, t.getObject)}.compare(x, y)
  }
}
