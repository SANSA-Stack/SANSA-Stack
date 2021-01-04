package net.sansa_stack.query.spark.graph.jena.util

import net.sansa_stack.query.spark.graph.jena.util.NodeType.NodeType
import org.apache.jena.graph._
import org.apache.spark.graphx.{EdgeTriplet, VertexId}

class MatchCandidate(
  val target: EdgeTriplet[Node, Node],
  val pattern: TriplePattern,
  nodeType: NodeType) extends Serializable {

  val isMatch: Boolean = pattern.isFulfilledByTriplet(target)
  val vertex: (VertexId, Node) = {
    nodeType match {
      case NodeType.s => (target.srcId, target.srcAttr)
      case NodeType.p => (-1L, target.attr)
      case NodeType.o => (target.dstId, target.dstAttr)
    }
  }
  val variable: Node = {
    nodeType match {
      case NodeType.s => pattern.getSubject
      case NodeType.p => pattern.getPredicate
      case NodeType.o => pattern.getObject
    }
  }
  val isVar: Boolean = variable.isVariable
  val mapping: Map[Node, Node] = Map(
    pattern.getSubject -> target.srcAttr,
    pattern.getPredicate -> target.attr,
    pattern.getObject -> target.dstAttr)

  override def toString: String = (variable, filterNotVariable(mapping), pattern).toString

  private def filterNotVariable(mapping: Map[Node, Node]): Map[Node, Node] = {
    mapping.filter(map => map._1.isVariable)
  }
}
