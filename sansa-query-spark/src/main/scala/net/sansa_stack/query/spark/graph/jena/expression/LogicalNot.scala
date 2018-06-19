package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node

/**
  * Class that execute filter with logical not.
  */
class LogicalNot(expr: Expression) extends FilterOne(expr) {

  private val tag = "Logical Not"

  private val filter = expr match {
    case e: Filter => e
    case _         => throw new java.lang.TypeNotPresentException("Filter",
      new Throwable("The input expression is not a type of filter"))
  }

  override def evaluate(solution: Map[Node, Node]): Boolean = {
    // compiler here
    true
  }

  override def evaluate(result: Result[Node]): Boolean = {
    !filter.evaluate(result)
  }

  override def getTag: String = { tag }
}
