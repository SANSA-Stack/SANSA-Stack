package net.sansa_stack.rdf.spark.partition.graph.utils

import scala.reflect.ClassTag

import org.apache.spark.graphx._

/**
 * A path List(e0,e1,...,em) is called an end-to-end path that, src of e0 is a source vertex
 * that has no incoming edges, dst of em is a sink vertex that has no outgoing edges.List all
 * end-to-end-paths in a graph.
 *
 * Remark: the situation of circles in graphs currently is not considered right now.
 *
 * @author Zhe Wang
 */
object EndToEndPaths extends Serializable {

  type Path[ED] = List[Edge[ED]]
  private def makeList[ED](e: Edge[ED]*): List[Path[ED]] = List(List(e: _*))

  /**
   * List all end-to-end-paths
   *
   * @tparam VD the vertex attribute type (not used in the computation)
   * @tparam ED the edge attribute type (not used in the computation)
   *
   * @param graph the input graph
   * @param maxIterations the maximum number of iterations to run for
   * @return a resulting graph
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], maxIterations: Int): VertexRDD[List[Path[ED]]] = {

    graph.cache()
    val src = setSrcVertices(graph)
    val dst = setDstVertices(graph)

    val pathGraph = graph.mapVertices[List[Path[ED]]] { (vid, _) =>
      if (dst.contains(vid)) {
        makeList(Edge(vid, vid))
      } else {
        makeList()
      }
    }

    val initialMessage = makeList[ED]()

    def vertexProgram(vid: VertexId, attr: List[Path[ED]], msg: List[Path[ED]]): List[Path[ED]] = {
      if (msg.head.isEmpty) {
        attr
      } else {
        if (src.contains(vid)) {
          if (attr.head.isEmpty) { msg }
          else { attr.++(msg) }
        } else { msg }
      }
    }

    def sendMessage(edge: EdgeTriplet[List[Path[ED]], ED]): Iterator[(VertexId, List[Path[ED]])] = {
      if (edge.dstAttr.head.isEmpty) {
        Iterator.empty
      } else {
        val edgeAdded = Edge(edge.srcId, edge.dstId, edge.attr)
        val attr = edge.dstAttr.map(path => path.+:(edgeAdded))
        Iterator((edge.srcId, attr))
      }
    }

    def mergeMessage(msg1: List[Path[ED]], msg2: List[Path[ED]]): List[Path[ED]] = {
      msg1.++(msg2)
    }

    Pregel.apply[List[Path[ED]], ED, List[Path[ED]]](
      pathGraph,
      initialMessage,
      maxIterations,
      activeDirection = EdgeDirection.In)(
      vertexProgram,
      sendMessage,
      mergeMessage).vertices
      .filter { case (vid, _) => src.contains(vid) }
      .mapValues(list => list.map(path => path.dropRight(1)))
  }

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
