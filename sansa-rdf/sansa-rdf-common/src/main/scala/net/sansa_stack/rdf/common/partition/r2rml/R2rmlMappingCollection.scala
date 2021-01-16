package net.sansa_stack.rdf.common.partition.r2rml

import org.apache.jena.rdf.model.Model

/**
 * A dedicated type for an R2RML model such that
 * implicit functions can be defined for it
 */
trait R2rmlMappingCollection { // TODO Consider renaming to R2rmlBasedPartitioning

  def getR2rmlModel(): Model

}
