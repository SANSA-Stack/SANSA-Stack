package net.sansa_stack.rdf.common.partition.core

import org.aksw.r2rml.jena.arq.lib.R2rmlLib
import org.aksw.r2rml.jena.domain.api.TriplesMap
import org.apache.jena.rdf.model.Model

/**
 * @author Lorenz Buehmann
 */
object RdfPartitionImportExport {

  /**
   * Imports the RDF partition states as `TriplesMap` from the given RDF data model.
   *
   * @param model the model
   * @return the RDF partition states as `TriplesMap`
   */
  def importFromR2RML(model: Model): Seq[TriplesMap] = {
    import collection.JavaConverters._
    R2rmlLib.streamTriplesMaps(model).iterator().asScala.toSeq
  }
}
