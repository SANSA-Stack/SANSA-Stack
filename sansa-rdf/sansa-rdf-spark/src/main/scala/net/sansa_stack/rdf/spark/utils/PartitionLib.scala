package net.sansa_stack.rdf.spark.utils

import net.sansa_stack.rdf.common.partition.core.{RdfPartitionStateDefault, RdfPartitioner}
import net.sansa_stack.rdf.common.partition.r2rml.R2rmlUtils
import net.sansa_stack.rdf.common.partition.utils.SQLUtils
import org.aksw.commons.sql.codec.util.SqlCodecUtils
import org.apache.jena.rdf.model.{Model, ModelFactory}

/**
 * @author Lorenz Buehmann
 */
object PartitionLib {
    /**
     * Exports the RDF partition states as R2RML.
     * Uses table name and sql escaping configuration suitable for spark.
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

      partitions.flatMap(partition =>
        R2rmlUtils.createR2rmlMappings(
          partitioner,
          partition,
          p => SQLUtils.createDefaultTableName(p),
          None,
          SqlCodecUtils.createSqlCodecDefault,
          model,
          explodeLanguageTags)
      )

      model
    }
}
