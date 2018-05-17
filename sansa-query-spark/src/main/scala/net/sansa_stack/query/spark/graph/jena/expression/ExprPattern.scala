package net.sansa_stack.query.spark.graph.jena.expression

import org.apache.jena.graph.Node

/**
  * Class that evaluate solution based on expression. Support expression as FILTER EXISTS { triple pattern }
  * @param op Type of operator.
  *
  * @author Zhe Wang
  */
class ExprPattern(variable: Node, value: Node, op: String) extends ExprFilter {

  private val tag = "Filter Pattern"

  override def evaluate(solution: Map[Node, Node]): Boolean = {
    true
  }

  override def getTag: String = { tag }
}
