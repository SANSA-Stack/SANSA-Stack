package net.sansa_stack.rdf.common.partition.core

import org.aksw.r2rml.jena.domain.api.TriplesMap
import org.aksw.r2rml.jena.vocab.RR
import org.apache.jena.rdf.model.{Model, ModelFactory}

import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils

import scala.collection.JavaConverters._

/**
 * @author Lorenz Buehmann
 */
object RdfPartitionImportExport {

  /**
   * Exports the RDF partition states as R2RML.
   *
   * @param partitioner the RDF partitioner
   * @param partitions the RDF partition states
   * @param explodeLanguageTags if `true` a separate mapping/TriplesMap will be created for each language tag,
   *                            otherwise a mapping to a column for the language tag represented by
   *                            `rr:langColumn` property will be used (note, this is an extension of R2RML)
   * @return the model containing the RDF partition states as as R2RML syntax
   */
  def exportAsR2RML(partitioner: RdfPartitioner[RdfPartitionStateDefault],
                    partitions: Seq[RdfPartitionStateDefault],
                    explodeLanguageTags: Boolean = false): Model = {
    // put all triple maps into a single model
    val model = ModelFactory.createDefaultModel()

    R2rmlUtils.createR2rmlMappings(partitioner, partitions, model, explodeLanguageTags)

    model
  }

  /**
   * Imports the RDF partition states as `TriplesMap` from the given RDF data model.
   *
   * @param model the model
   * @return the RDF partition states as `TriplesMap`
   */
  def importFromR2RML(model: Model): Seq[TriplesMap] = {
    model.listResourcesWithProperty(RR.logicalTable).mapWith(_.as(classOf[TriplesMap])).asScala.toSeq
  }
}
