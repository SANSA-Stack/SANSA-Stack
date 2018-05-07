package net.sansa_stack.query.spark.graph.jena.expression

import org.apache.jena.graph.Node

trait ExprFilter extends Serializable {

  def evaluate(solution: Map[Node, Node]): Boolean

  def getTag: String

}
