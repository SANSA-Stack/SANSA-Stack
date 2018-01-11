package net.sansa_stack.rdf.spark.io.ntriples

import java.io.ByteArrayInputStream

import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr}

/**
  * Convert an N-Triples line to a Jena [[Triple]] object.
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
