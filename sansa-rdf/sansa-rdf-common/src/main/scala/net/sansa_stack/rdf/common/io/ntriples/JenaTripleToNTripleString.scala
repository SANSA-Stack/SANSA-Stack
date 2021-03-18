package net.sansa_stack.rdf.common.io.ntriples

import org.apache.jena.graph.Triple
import org.apache.jena.sparql.util.FmtUtils


/**
  * Convert a Jena Triple to an N-Triples string.
  *
  * @author Lorenz Buehmann
  */
class JenaTripleToNTripleString
    extends (Triple => String)
    with java.io.Serializable {
  override def apply(t: Triple): String = s"${FmtUtils.stringForTriple(t)} ."
}
