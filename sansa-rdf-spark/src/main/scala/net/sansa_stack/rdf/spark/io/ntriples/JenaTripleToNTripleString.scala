package net.sansa_stack.rdf.spark.io.ntriples

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
    val objStr =
      if (t.getObject.isLiteral) {
        t.getObject
      } else {
        s"<${t.getObject}>"
      }
    s"<${t.getSubject}> <${t.getPredicate}> ${objStr} ."
  }
}
