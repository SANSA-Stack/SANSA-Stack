package net.sansa_stack.rdf.spark.partition.graph.algo

import net.sansa_stack.rdf.spark.partition.graph.utils.{EndToEndPaths, StartVerticesGroup, VertexAttribute}
import org.apache.spark.graphx._
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
class PathPartition[VD: ClassTag,ED: ClassTag](
    override val graph: Graph[VD,ED],
    override val session: SparkSession,
    override val numPartitions: PartitionID,
    private var numIterations: Int)
  extends PartitionAlgo(graph,session,numPartitions) with Serializable {

  /**
    * Constructs a default instance with default parameters {numPartitions: graph partitions, numIterations: 5}
    *
    */
  def this(graph: Graph[VD,ED], session: SparkSession) = {
    this(graph, session, graph.edges.partitions.length, 5)
  }

  /**
    * Constructs a default instance with default parameters {numIterations: 5}
    *
    */
  def this(graph: Graph[VD,ED], session: SparkSession, numPartitions: Int) = {
    this(graph, session, numPartitions, 5)
  }

  /**
    * Sets the number of iterations (default: 5)
    *
    */
  def setNumIterations(numIterations: Int): this.type = {
    require(numIterations > 0,
      s"Number of iterations must be positive but got $numIterations")
    this.numIterations = numIterations
    this
  }

  graph.cache()
  private val paths = EndToEndPaths.run(graph,numIterations)
  private val sources = EndToEndPaths.setSrcVertices(graph)
  private val vertexAttr = VertexAttribute.apply(graph, paths, session.sparkContext)
  private val svg = StartVerticesGroup.apply(graph, vertexAttr, numPartitions)

  override def partitionBy(): Graph[VD,ED] = {
    val broadcast = session.sparkContext.broadcast(paths.collect())
    val bhp = graph.partitionBy(PartitionStrategy.RandomVertexCut, numPartitions)
    val newEdges = bhp.edges.mapPartitionsWithIndex{ case(pid,_) =>
      broadcast.value.filter{ case(vid,_) =>
        sources.filter(getPartition(_) == pid).contains(vid)
      }.flatMap{ case(_,it) => it }.flatten.distinct.toIterator
    }.cache()

    Graph[VD,ED](graph.vertices,newEdges)
  }

  private def getPartition(src:VertexId) : PartitionID = {
    svg.flatMap(array =>
      array.flatMap(vid =>
        Map(vid -> svg.indexOf(array))
      )
    ).toMap.getOrElse(src,numPartitions)
  }
}