package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node

/**
 * Class that manipulate expression with bound operators.
 */
class Bound(expr: Expression) extends FilterOne(expr) {

  private val tag = "Bound"

  private val node = expr match {
    case e: NodeVar => e.getNode
    case _ => throw new TypeNotPresentException(
      "Expression Variable",
      new Throwable("The input expression is not a type of variable"))
  }

  override def evaluate(solution: Map[Node, Node]): Boolean = {
    if (solution.keySet.contains(node)) {
      true
    } else {
      false
    }
  }

  override def evaluate(solution: Result[Node]): Boolean = {
    solution.getField.contains(node)
  }

  override def getTag: String = { tag }
}
