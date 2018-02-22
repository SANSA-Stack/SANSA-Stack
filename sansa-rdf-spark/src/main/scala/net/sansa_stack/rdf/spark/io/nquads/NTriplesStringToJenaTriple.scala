package net.sansa_stack.rdf.spark.io.ntriples

import java.io.ByteArrayInputStream

import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.Quad
import org.apache.jena.riot.{Lang, RDFDataMgr}

/**
  * Convert an N-Quads line to a Jena [[Quad]] object.
  *
  * @author Gezim Sejdiu
  */
class NQuadsStringToJenaQuad
    extends Function1[String, Quad]
    with java.io.Serializable {
  override def apply(s: String): Quad = {
    RDFDataMgr.createIteratorQuads(new ByteArrayInputStream(s.getBytes), Lang.NQUADS, null).next()
  }
}
