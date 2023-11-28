package net.sansa_stack.query.spark.graph.jena.util

import org.apache.jena.graph._
import org.apache.spark.graphx.EdgeTriplet

class TriplePattern(
  private val s: Node,
  private val p: Node,
  private val o: Node) extends Serializable {

  def this(triple: Triple) = {
    this(triple.getSubject, triple.getPredicate, triple.getObject)
  }

  def this(tp: TriplePattern) = {
    this(tp.s, tp.p, tp.o)
  }

  def getSubject: Node = {
    s
  }

  def getSubjectIsVariable: Boolean = {
    s.isVariable
  }

  def getPredicate: Node = {
    p
  }

  def getPredicateIsVariable: Boolean = {
    p.isVariable
  }

  def getObject: Node = {
    o
  }

  def getObjectIsVariable: Boolean = {
    o.isVariable
  }

  def isFulfilledByTriplet(triplet: EdgeTriplet[Node, Node]): Boolean = {
    checkQueryPattern(s, triplet.srcAttr) && checkQueryPattern(p, triplet.attr) && checkQueryPattern(o, triplet.dstAttr)
  }

  private def checkQueryPattern(pattern: Node, target: Node): Boolean = {
    if (pattern.isVariable) {
      true
    } else {
      pattern.equals(target)
    }
  }

  def getVariable: Array[Node] = {
    Array[Node](s, p, o).filter(node => node.isVariable)
  }

  def compares(obj: TriplePattern): Boolean = {
    this.s.equals(obj.s) && this.p.equals(obj.p) && this.o.equals(obj.o)
  }

  override def toString: String = { s.toString() + " " + p.toString() + " " + o.toString() + " ." }

  override def hashCode(): Int = {
    s.hashCode() * 5 + p.hashCode() * 3 + o.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    this.hashCode() == obj.hashCode()
  }
}
