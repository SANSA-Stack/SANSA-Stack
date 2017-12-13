package net.sansa_stack.inference.utils

import org.apache.jena.graph.Triple


/**
  * Convert a Jena Triple to an N-Triples string.
  *
  * @author Lorenz Buehmann
  */
class JenaTripleToNTripleString
    extends ((Triple) => String)
    with java.io.Serializable {
  override def apply(t: Triple): String = {
    val subStr =
      if (t.getSubject.isBlank) {
        s"_:${t.getSubject.getBlankNodeLabel}"
      } else {
        s"<${t.getSubject.getURI}>"
      }

    val objStr =
      if (t.getObject.isLiteral) {
        t.getObject
      } else if (t.getObject.isBlank) {
        s"_:${t.getObject}"
      } else {
        s"<${t.getObject}>"
      }
    s"${subStr} <${t.getPredicate}> ${objStr} ."
  }
}

