package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node

class LogicalAnd(left: Expression, right: Expression) extends FilterTwo(left, right) {

  private val tag = "Logical And"
  private val leftFilter = left match {
    case e: Filter => e
    case _ => throw new TypeNotPresentException(
      "Filter",
      new Throwable("The input left expression is not a type of filter"))
  }
  private val rightFilter = right match {
    case e: Filter => e
    case _ => throw new TypeNotPresentException(
      "Filter",
      new Throwable("The input left expression is not a type of filter"))
  }

  override def evaluate(result: Map[Node, Node]): Boolean = {
    // compiler here
    throw new UnsupportedOperationException
  }

  override def evaluate(result: Result[Node]): Boolean = {
    leftFilter.evaluate(result) && rightFilter.evaluate(result)
  }

  override def getTag: String = { tag }
}
