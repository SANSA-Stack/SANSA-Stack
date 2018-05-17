package net.sansa_stack.query.spark.graph.jena.util

import net.sansa_stack.query.spark.graph.jena.util.MatchCandidate.NodeType
import org.apache.jena.graph._
import org.apache.spark.graphx.{EdgeTriplet, VertexId}

class MatchCandidateNode(val target: EdgeTriplet[Node, Node],
                         val pattern: TriplePatternNode,
                         nodeType: NodeType) extends Serializable {

  val isMatch: Boolean = pattern.isFulfilledByTriplet(target)
  val vertex: (VertexId, Node) = {
    nodeType match {
      case MatchCandidate.s => (target.srcId, target.srcAttr)
      case MatchCandidate.p => (-1L, target.attr)
      case MatchCandidate.o => (target.dstId, target.dstAttr)
    }
  }
  val variable: Node = {
    nodeType match {
      case MatchCandidate.s => pattern.getSubject
      case MatchCandidate.p => pattern.getPredicate
      case MatchCandidate.o => pattern.getObject
    }
  }
  val isVar: Boolean = variable.isVariable
  val mapping: Map[Node,Node] = Map(
    pattern.getSubject->target.srcAttr,
    pattern.getPredicate->target.attr,
    pattern.getObject->target.dstAttr)

  override def toString: String = (vertex, variable, mapping, pattern).toString
}

object MatchCandidate extends Enumeration {
  type NodeType = Value
  val s, p, o = Value
}