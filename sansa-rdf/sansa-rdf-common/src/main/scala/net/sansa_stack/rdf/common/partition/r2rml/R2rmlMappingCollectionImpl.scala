package net.sansa_stack.rdf.common.partition.r2rml

import org.apache.jena.rdf.model.Model

class R2rmlMappingCollectionImpl(val r2rmlModel: Model)
  extends R2rmlMappingCollection with Serializable {
  override def getR2rmlModel(): Model = r2rmlModel
}
