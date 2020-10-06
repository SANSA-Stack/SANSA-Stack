package net.sansa_stack.inference.utils

import com.google.common.collect.ComparisonChain

import net.sansa_stack.inference.data.RDFTriple

/**
  * Comparator for RDF triples.
  *
  * @author Lorenz Buehmann
  */
object RDFTripleOrdering extends Ordering[RDFTriple]{
  override def compare(t1: RDFTriple, t2: RDFTriple): Int =
    ComparisonChain.start()
      .compare(t1.s, t2.s)
      .compare(t1.p, t2.p)
      .compare(t1.o, t2.o)
      .result()
}
