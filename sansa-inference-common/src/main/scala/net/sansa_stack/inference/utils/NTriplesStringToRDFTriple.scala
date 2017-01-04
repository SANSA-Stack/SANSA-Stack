package net.sansa_stack.inference.utils

import net.sansa_stack.inference.data.RDFTriple

/**
  * Convert an N-Triples line to an RDFTriple object.
  *
  * @author Lorenz Buehmann
  */
class NTriplesStringToRDFTriple
    extends Function1[String, RDFTriple]
    with java.io.Serializable {
  override def apply(s: String): RDFTriple = {
    val tokens = s.replace("<", "").replace(">", "").split(" ") // split by white space
    RDFTriple(tokens(0), tokens(1), tokens(2))
  }
}
