package net.sansa_stack.rdf.spark.partition.graph.utils

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag
/**
  * A path List(e0,e1,...,em) is called an end-to-end path that, src of e0 is a source vertex that has no incoming edges,
  * dst of em is a sink vertex that has no outgoing edges.List all end-to-end-paths in a graph.
  * @param graph the input graph
  * @param maxIterations the maximum number of iterations to run for
  * @tparam VD the vertex attribute type (not used in the computation)
  * @tparam ED the edge attribute type (not used in the computation)
  */
class Paths[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                        maxIterations: Int,
                                        session: SparkSession) extends Serializable {

  val src: Broadcast[Array[VertexId]] = session.sparkContext.broadcast(Paths.setSrcVertices(graph))

  val dst: Broadcast[Array[VertexId]] = session.sparkContext.broadcast(Paths.setDstVertices(graph))

  type Path = Seq[Edge[ED]]

  private def makeList(e: Edge[ED]*): List[Path] = List(Seq(e: _*))

  private val g = graph.mapVertices[List[Path]] { (vid, _) =>
    if (dst.value.contains(vid)) {
      makeList(Edge(vid, vid))
    } else {
      makeList()
    }
  }.cache()

  private val initialMessage = makeList()

  private def vertexProgram(vid: VertexId, attr: List[Path], msg: List[Path]): List[Path] = {
    if (msg.head.isEmpty) {
      attr
    } else {
      if (src.value.contains(vid)) {
        if (attr.head.isEmpty) { msg }
        else { attr.++(msg) }
      } else { msg }
    }
  }

  private def sendMessage(edge: EdgeTriplet[List[Path], ED]): Iterator[(VertexId, List[Path])] = {
    if (edge.dstAttr.head.isEmpty) {
      Iterator.empty
    } else {
      val edgeAdded = Edge(edge.srcId, edge.dstId, edge.attr)
      val attr = edge.dstAttr.map(path => path.+:(edgeAdded))
      Iterator((edge.srcId, attr))
    }
  }

  private def mergeMessage(msg1: List[Path], msg2: List[Path]): List[Path] = {
    msg1.++(msg2)
  }

  val pathGraph: Graph[List[Path], ED] = Pregel.apply[List[Path], ED, List[Path]](
    g,
    initialMessage,
    maxIterations,
    activeDirection = EdgeDirection.In)(
    vertexProgram,
    sendMessage,
    mergeMessage).mapVertices((vid, list) =>
    if (src.value.contains(vid)) {
    list.map(path => path.dropRight(1))} else {
    makeList()})
}

object Paths {
  def setSrcVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Array[VertexId] = {
    val ops = graph.ops
    val src = graph.vertices.map(v => v._1).subtract(ops.inDegrees.map(v => v._1)).collect()
    src
  }

  def setDstVertices[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Array[VertexId] = {
    val ops = graph.ops
    val dst = graph.vertices.map(v => v._1).subtract(ops.outDegrees.map(v => v._1)).collect()
    dst
  }
}
