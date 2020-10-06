package net.sansa_stack.rdf.spark.partition.graph.utils

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag
/**
  * Construct triple groups for input vertices
  *
  * Subject-based triples groups: s-TG of vertex v\inV is a set of triples in which their subject is v
  * denoted by s-TG(v)= {(u,w)\(u,w)\inE, u = v}
  *
  * Object-based triples groups: o-TG of vertex v\inV is a set of triples in which their object is v
  * denoted by s-TG(v)= {(u,w)\(u,w)\inE, w = v}
  *
  * Subject-object-based triple groups: so-TG of vertex v\inV is a set of triples in which their object is v
  * denoted by s-TG(v)= {(u,w)\(u,w)\inE, v\in{u,w}}
  *
  * @author Zhe Wang
  */
class Groups[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                         maxIterations: Int,
                                         session: SparkSession) extends Serializable {
  type group = List[Edge[ED]]

  private def makeList(e: Edge[ED]*): group = List(e: _*)

  private val g = graph.mapVertices[group] { (_, _) =>
    makeList()
  }

  private val initialMessage = makeList()

  private def vertexProgram(vid: VertexId, attr: group, msg: group): group = {
    attr.++(msg).distinct
  }

  private def sendMsgToSrc(edge: EdgeTriplet[group, ED]): Iterator[(VertexId, group)] = {
    val edgeAdded = Edge(edge.srcId, edge.dstId, edge.attr)
    val attr = edge.dstAttr.+: (edgeAdded)
    Iterator((edge.srcId, attr))
  }

  private def sendMsgToDst(edge: EdgeTriplet[group, ED]): Iterator[(VertexId, group)] = {
    val edgeAdded = Edge(edge.srcId, edge.dstId, edge.attr)
    val attr = edge.srcAttr.+: (edgeAdded)
    Iterator((edge.dstId, attr))
  }

  private def sendMsgToBoth(edge: EdgeTriplet[group, ED]): Iterator[(VertexId, group)] = {
    val edgeAdded = Edge(edge.srcId, edge.dstId, edge.attr)
    val attrSrc = edge.dstAttr.+: (edgeAdded)
    val attrDst = edge.srcAttr.+: (edgeAdded)
    Iterator((edge.srcId, attrSrc), (edge.dstId, attrDst))
  }

  private def mergeMessage(msg1: group, msg2: group): group = {
    msg1.++(msg2)
  }

  lazy val subjectGroup: Graph[group, ED] = Pregel.apply[group, ED, group](
    g,
    initialMessage,
    maxIterations,
    activeDirection = EdgeDirection.In)(
    vertexProgram,
    sendMsgToSrc,
    mergeMessage)

  lazy val objectGroup: Graph[group, ED] = Pregel.apply[group, ED, group](
    g,
    initialMessage,
    maxIterations,
    activeDirection = EdgeDirection.Out)(
    vertexProgram,
    sendMsgToDst,
    mergeMessage)

  lazy val bothGroup: Graph[group, ED] = Pregel.apply[group, ED, group](
    g,
    initialMessage,
    maxIterations,
    activeDirection = EdgeDirection.Both)(
    vertexProgram,
    sendMsgToBoth,
    mergeMessage)
}
