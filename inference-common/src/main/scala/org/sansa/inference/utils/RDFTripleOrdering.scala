package org.sansa.inference.utils

import com.google.common.collect.ComparisonChain
import org.sansa.inference.data.RDFTriple

/**
  * Comparator for RDF triples.
  *
  * @author Lorenz Buehmann
  */
object RDFTripleOrdering extends Ordering[RDFTriple]{
  override def compare(t1: RDFTriple, t2: RDFTriple): Int =
    ComparisonChain.start()
      .compare(t1.subject, t2.subject)
      .compare(t1.predicate, t2.predicate)
      .compare(t1.`object`, t2.`object`)
      .result()
}
