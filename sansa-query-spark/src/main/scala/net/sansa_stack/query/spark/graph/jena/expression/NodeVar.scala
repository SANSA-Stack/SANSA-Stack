package net.sansa_stack.query.spark.graph.jena.expression

import org.apache.jena.graph.Node

/**
  * Serializable class that store expression variable.
  * @param exprVar variable
  */
class NodeVar(exprVar: Node) extends Expression {

  private val tag = "Node Variable"

  def getNode: Node = { exprVar }

  override def getTag: String = { tag }

  override def toString: String = { exprVar.toString }

  override def equals(obj: scala.Any): Boolean = { this.getNode.equals(obj.asInstanceOf[NodeVar].getNode) }

  override def hashCode(): Int = { exprVar.hashCode }
}
