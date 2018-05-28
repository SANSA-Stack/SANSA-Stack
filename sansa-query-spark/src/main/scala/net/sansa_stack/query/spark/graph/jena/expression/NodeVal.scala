package net.sansa_stack.query.spark.graph.jena.expression

import org.apache.jena.graph.Node

class NodeVal(nodeValue: Node) extends Expression {

  private val tag = "Node Value"

  def getNode: Node = { nodeValue }

  override def getTag: String = { tag }

  override def toString: String = { nodeValue.toString }

  override def equals(obj: scala.Any): Boolean = { this.getNode.equals(obj.asInstanceOf[NodeVar].getNode) }

  override def hashCode(): Int = { nodeValue.hashCode }
}
