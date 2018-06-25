package net.sansa_stack.rdf.spark.partition.graph.algo

import net.sansa_stack.rdf.spark.partition.graph.utils.{Paths, VerticesPlacement}
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.{Graph, PartitionID}
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag
/**
  * Path Partition Strategy expand the partitions by assign path groups of all start vertices
  * Expand Edges set E+(i) = pg(sv*).edges
  *
  * @param graph target graph to be partitioned
  * @param session spark session
  * @param numPartitions number of partitions
  * @tparam VD the vertex attribute associated with each vertex in the set.
  * @tparam ED the edge attribute associated with each edge in the set.
  *
  * @author Zhe Wang
  */
class PathPartition[VD: ClassTag, ED: ClassTag](override val graph: Graph[VD, ED],
                                                override val session: SparkSession,
                                                override val numPartitions: PartitionID,
                                                private var numIterations: Int)
  extends PartitionAlgo(graph, session, numPartitions) with Serializable {

  /**
    * Constructs a default instance with default parameters {numPartitions: graph partitions, numIterations: 5}
    *
    */
  def this(graph: Graph[VD, ED], session: SparkSession) = {
    this(graph, session, graph.edges.partitions.length, 5)
  }

  /**
    * Constructs a default instance with default parameters {numIterations: 5}
    *
    */
  def this(graph: Graph[VD, ED], session: SparkSession, numPartitions: Int) = {
    this(graph, session, numPartitions, 5)
  }

  /**
    * Sets the number of iterations (default: 5)
    *
    */
  def setNumIterations(numIterations: Int): this.type = {
    require(
      numIterations > 0,
      s"Number of iterations must be positive but got $numIterations")
    this.numIterations = numIterations
    this
  }

  val paths = new Paths(graph, numIterations, session)

  override def partitionBy(): Graph[VD, ED] = {
    val g = paths.pathGraph.cache()
    val verticesPartMap = session.sparkContext.broadcast(VerticesPlacement.placeByIndex(paths.src.value, numPartitions))
    val edges = g.vertices.filter(_._2.head.nonEmpty).flatMap { case (vid, paths) =>
      val pid = verticesPartMap.value(vid)
      paths.flatMap(_.toIterator).map(e => (pid, e)) }
      .distinct()
      .partitionBy(new HashPartitioner(numPartitions))
      .map(_._2).cache()
    val vertices = graph.vertices.cache()
    g.unpersist()
    Graph.apply(vertices, edges)
  }
}
