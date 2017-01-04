package net.sansa_stack.inference.utils

import net.sansa_stack.inference.data.RDFTriple

/**
  * Convert an RDFTriple object to an N-Triple string.
  *
  * @author Lorenz Buehmann
  */
class RDFTripleToNTripleString
    extends Function1[RDFTriple, String]
    with java.io.Serializable {
  override def apply(t: RDFTriple): String = {
    val objStr =
      if (t.`object`.startsWith("http:") || t.`object`.startsWith("ftp:")) {
        s"<${t.`object`}>"
      } else {
        t.`object`
      }
    s"<${t.subject}> <${t.predicate}> ${objStr} ."
  }
}
