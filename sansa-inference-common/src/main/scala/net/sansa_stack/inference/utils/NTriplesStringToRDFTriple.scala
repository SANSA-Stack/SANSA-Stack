package net.sansa_stack.inference.utils

import java.io.ByteArrayInputStream

import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr}

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
    val t = RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(s.getBytes), Lang.NTRIPLES, null).next()
    RDFTriple(t.getSubject.toString, t.getPredicate.toString, t.getObject.toString)
  }
}
