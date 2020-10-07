package net.sansa_stack.rdf.spark.io.nquads

import java.io.ByteArrayInputStream

import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.sparql.core.Quad

/**
  * Convert an N-Quads line to a Jena [[org.apache.jena.sparql.core.Quad]] object.
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
