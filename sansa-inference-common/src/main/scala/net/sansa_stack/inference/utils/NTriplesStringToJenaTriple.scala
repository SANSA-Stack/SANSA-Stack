package net.sansa_stack.inference.utils

import java.io.ByteArrayInputStream

import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr}

/**
  * Convert an N-Triples line to an RDFTriple object.
  *
  * @author Lorenz Buehmann
  */
class NTriplesStringToJenaTriple
    extends Function1[String, Triple]
    with java.io.Serializable {
  override def apply(s: String): Triple = {
    RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(s.getBytes), Lang.NTRIPLES, null).next()
  }
}
