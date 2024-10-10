package net.sansa_stack.rdf.common.partition.core

import net.sansa_stack.rdf.common.partition.layout.TripleLayout
import org.apache.jena.graph.Triple

/**
 * A partition implementation based on a partitioner and a partition state
 *
 * special datatypes: b for blank, u for uri, typed literal otherwise
 */
/*
case class RdfPartitionFromStateImpl[S](state: S, partitioner: RdfPartitioner[S])
  extends RdfPartitionFromState[S]
    with Serializable {
  def layout: TripleLayout = partitioner.determineLayout(state)
  def matches(t: Triple): Boolean = partitioner.matches(state, t)
}
*/

