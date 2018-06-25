package net.sansa_stack.rdf.spark.partition.graph.algo

import net.sansa_stack.rdf.spark.partition.graph.utils.{Groups, VerticesPlacement}
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.{Graph, PartitionID}
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

/**
 * Expanding the partitions by assigning object triple group of each vertex to partitions
 *
 * @param graph target graph to be partitioned
 * @param session spark session
 * @param numPartitions number of partitions
 * @param numIterations number of iterations
 * @tparam VD the vertex attribute associated with each vertex in the set.
 * @tparam ED the edge attribute associated with each edge in the set.
 *
 * @author Zhe Wang
 */
class ObjectHashPartition[VD: ClassTag, ED: ClassTag](
  override val graph: Graph[VD, ED],
  override val session: SparkSession,
  override val numPartitions: PartitionID,
  private var numIterations: Int)
  extends PartitionAlgo(graph, session, numPartitions) with Serializable {

  /**
   * Constructs a default instance with default parameters {numPartitions: graph partitions, numIterations: 2}
   *
   */
  def this(graph: Graph[VD, ED], session: SparkSession) = {
    this(graph, session, graph.edges.partitions.length, 2)
  }

  /**
   * Constructs a default instance with default parameters {numIterations: 2}
   *
   */
  def this(graph: Graph[VD, ED], session: SparkSession, numPartitions: Int) = {
    this(graph, session, numPartitions, 2)
  }

  /**
   * Sets the number of iterations (default: 2)
   *
   */
  def setNumIterations(numIterations: Int): this.type = {
    require(
      numIterations > 0,
      s"Number of iterations must be positive but got $numIterations")
    this.numIterations = numIterations
    this
  }

  val groups = new Groups(graph, numIterations, session)

  override def partitionBy(): Graph[VD, ED] = {
    val g = groups.objectGroup.cache()
    val edges = g.vertices.flatMap{ case(vid, group) =>
      val pid = VerticesPlacement.placeById(vid, numPartitions)
      group.map(e => (pid, e))}
      .distinct().partitionBy(new HashPartitioner(numPartitions))
      .map(_._2).cache()
    val vertices = graph.vertices.cache()
    g.unpersist()
    Graph.apply(vertices, edges)
  }
}
