package net.sansa_stack.rdf.flink

import net.sansa_stack.rdf.common.partition.core.RdfPartitionDefault
import net.sansa_stack.rdf.flink.partition.core.RdfPartitionUtilsFlink
import net.sansa_stack.rdf.flink.partition.semantic.SemanticRdfPartitionUtilsFlink
import org.apache.flink.api.scala.{ DataSet, _ }
import org.apache.jena.graph.Triple

/**
 * Wrap up implicit classes/methods to partition RDF data from N-Triples
 * files into either [[Sparqlify]] or [[Semantic]] partition strategies.
 *
 * @author Gezim Sejdiu
 */
package object partition {

  implicit class RDFPartition(triples: DataSet[Triple]) extends Serializable {

    /**
     * Default partition - using VP.
     */
    def partitionGraph(): Map[RdfPartitionDefault, DataSet[Product]] = {
      RdfPartitionUtilsFlink.partitionGraph(triples)
    }

    /**
     * semantic partition of and RDF graph
     */
    def partitionGraphAsSemantic(): DataSet[String] = {
      SemanticRdfPartitionUtilsFlink.partitionGraph(triples)
    }
  }
}
