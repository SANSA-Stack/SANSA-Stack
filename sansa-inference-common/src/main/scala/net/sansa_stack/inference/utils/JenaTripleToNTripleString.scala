package net.sansa_stack.inference.utils

import org.apache.jena.graph.Triple
import org.apache.jena.shared.PrefixMapping
import org.apache.jena.sparql.util.FmtUtils


/**
  * Convert a Jena Triple to an N-Triples string.
  *
  * @note it turns out, that it might be more efficient to use the Jena stream based writer API per partition.
  *
  * @author Lorenz Buehmann
  */
class JenaTripleToNTripleString
    extends Function[Triple, String]
    with java.io.Serializable {

  override def apply(t: Triple): String = s"${FmtUtils.stringForTriple(t, null.asInstanceOf[PrefixMapping])} ."
}

