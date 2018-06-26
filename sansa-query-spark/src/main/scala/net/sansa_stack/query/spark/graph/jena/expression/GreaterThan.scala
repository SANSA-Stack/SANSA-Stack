package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node

class GreaterThan(left: Expression, right: Expression) extends FilterTwo(left, right) {

  private val tag = "Greater Than"

  override def evaluate(result: Map[Node, Node]): Boolean = {
    // compiler here
    true
  }

  override def evaluate(result: Result[Node]): Boolean = {
    compareNodes(result) > 0
  }

  override def getTag: String = { tag }
}
