package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.{Node, NodeFactory}

class Add(left: Expression, right: Expression) extends FunctionTwo(left, right) {

  private val tag = "Add"

  override def getValue(result: Map[Node, Node]): Node = {
    // compiler here
    throw new UnsupportedOperationException()
  }

  override def getValue(result: Result[Node]): Node = {
    val leftAndRight = getLeftAndRightValue(result)
    val leftValue = leftAndRight._1
    val rightValue = leftAndRight._2
    val value = NodeFactory.createLiteral(
      (leftValue.getLiteralValue.toString.toDouble + rightValue.getLiteralValue.toString.toDouble).toString)
    value
  }

  override def getTag: String = { tag }
}
