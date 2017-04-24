package net.sansa_stack.inference.flink.extraction

import org.apache.jena.vocabulary.RDFS

/**
  * An extractor of the schema for RDFS.
  *
  * Currently, it's supports the extraction of triples `(s,p,o)` with `p` being
  *
  *  - rdfs:subClassOf
  *  - rdfs:subPropertyOf
  *  - rdfs:domain
  *  - rdfs:range
  *
  * @author Lorenz Buehmann
  */
class RDFSSchemaExtractor()
  extends SchemaExtractor()(Set(RDFS.subClassOf, RDFS.subPropertyOf, RDFS.domain, RDFS.range).map(p => p.getURI))() {}

object RDFSSchemaExtractor {
  def apply: RDFSSchemaExtractor = new RDFSSchemaExtractor()
}
