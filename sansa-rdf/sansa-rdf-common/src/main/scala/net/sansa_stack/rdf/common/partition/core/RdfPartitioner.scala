package net.sansa_stack.rdf.common.partition.core

import net.sansa_stack.rdf.common.partition.layout.TripleLayout
import org.apache.jena.graph.Triple

/**
 * An RdfPartitioner bundles the following functionality:
 * <ul>
 *   <li>Mapping of triples to partition objects (of generic type "S")</li>
 *   <li>Testing for whether a given triple would fit into the partition denoted
 *       by an instance of "S"</li>
 *   <li>Mapping of partition objects to TripleLayouts.
 *       TripleLayouts correspond to a table with a certain scala-based schema
 *       and they can map triples to tuples suitable for serving as rows w.r.t. the schema</li>
 * </ul>
 *
 * @tparam S the partition type handled by the partitioner
 */
trait RdfPartitioner[S] {
  /** Create a partition state from the given triple */
  def fromTriple(triple: Triple): S

  /** Yield a TripleLayout for a partition state */
  def determineLayout(partition: S): TripleLayout

  /** Test whether the given triple falls into to same partition */
  def matches(partition: S, triple: Triple): Boolean = {
    val newPartition = fromTriple(triple)
    newPartition.equals(partition)
  }

  /**
   * Aggregate partitions.
   *
   * @param partitions the partitions
   * @return the aggregated partitions
   */
  def aggregate(partitions: Seq[S]): Seq[S]
}

