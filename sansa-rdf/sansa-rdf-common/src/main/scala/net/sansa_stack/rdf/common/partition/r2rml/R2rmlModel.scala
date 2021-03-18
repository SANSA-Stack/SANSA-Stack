package net.sansa_stack.rdf.common.partition.r2rml

import org.apache.jena.rdf.model.Model

/**
 * A dedicated type for designating a plain Jena Model to contain R2RML mappings
 * such that (implicit) functions definitions or dependency injection can unambiguously
 * rely upon
 */
case class R2rmlModel(r2rmlModel: Model) extends Serializable {
}
