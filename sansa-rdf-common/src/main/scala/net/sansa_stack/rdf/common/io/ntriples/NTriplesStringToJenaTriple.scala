package net.sansa_stack.rdf.common.io.ntriples

import java.io.ByteArrayInputStream

import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr}

/**
 * Convert an N-Triples line to a Jena [[Triple]] object.
 *
 * @note
 * This function isn't really efficient because for each triple a new iterator object with
 * all its parsing dependencies will be created.
 * It should be more efficient to create a single parser per e.g. partition and process a set of lines
 * at once.
 *
 * @author Lorenz Buehmann
 */
class NTriplesStringToJenaTriple
  extends (String => Triple)
    with java.io.Serializable {
  override def apply(s: String): Triple = {
    RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(s.getBytes), Lang.NTRIPLES, null).next()
  }
}
