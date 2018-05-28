package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node

/**
  * Class that evaluate solution based on expression. Support expression as FILTER EXISTS { triple pattern }
  * @param op Type of operator.
  *
  * @author Zhe Wang
  */
class Pattern(variable: Node, value: Node, op: String) extends Filter {

  private val tag = "Filter Pattern"

  override def evaluate(solution: Map[Node, Node]): Boolean = {
    true
  }

  override def evaluate(solution: Result[Node]): Boolean = {
    true
  }

  override def getTag: String = { tag }
}
