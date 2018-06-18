package net.sansa_stack.rdf.spark.partition.graph.evaluation

import scala.reflect.ClassTag

import net.sansa_stack.rdf.spark.partition.graph.algo.PartitionAlgo

/**
 * Evaluator for partition strategy.
 *
 * @param ps Partition Strategy
 * @tparam VD the vertex attribute associated with each vertex in the set.
 * @tparam ED the edge attribute associated with each edge in the set.
 *
 * @author Zhe Wang
 */
class PartitionAlgoMetrics[VD: ClassTag, ED: ClassTag](
  ps: PartitionAlgo[VD, ED]) extends Serializable {

  private lazy val numPartitions = ps.numPartitions

  /**
   * Denote the ratio of a partition strategy between the total number of edges
   * in partitioned graph and those in original graph
   *
   */
  def duplication(): Double = {
    numTotalEdges.toDouble / numDistinctEdges.toDouble
  }

  /**
   * Denote the ratio between the maximum number of edges in one partition and the
   * average number of edges in all partitions
   *
   */
  def balance(): Double = {
    val avgEdges = partitionedGraph.edges.count.toDouble / numPartitions
    numMaxEdges.toDouble / avgEdges
  }

  /**
   * Denote percentage of edges that not replicated from original graph to partitioned graph
   *
   */
  def efficiency(): Double = {
    numMergedEdges.toDouble / numDistinctEdges.toDouble
  }

  private lazy val partitionedGraph = ps.partitionBy().cache()
  private lazy val numDistinctEdges = ps.graph.edges.count
  private lazy val numTotalEdges = partitionedGraph.edges.count
  private lazy val numMergedEdges = partitionedGraph.edges.countByValue().count { case (_, num) => num == 1 }
  private lazy val numMaxEdges = partitionedGraph.edges.aggregate(0)((x, _) => x + 1, (x, y) => Math.max(x, y))
}
