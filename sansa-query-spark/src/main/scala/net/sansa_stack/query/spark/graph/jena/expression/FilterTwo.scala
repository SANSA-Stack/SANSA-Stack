package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node
import org.apache.jena.sparql.expr.NodeValue

abstract class FilterTwo(protected val left: Expression, protected val right: Expression) extends Filter {

  protected def compareNodes(result: Result[Node]): Int = {
    var leftValue: String = ""
    var rightValue: String = ""

    // Left side is variable and right side is value
    if (left.isInstanceOf[NodeVar] && right.isInstanceOf[NodeVal]) {
      val leftVar = left.asInstanceOf[NodeVar].getNode
      val rightVal = right.asInstanceOf[NodeVal].getNode
      if (rightVal.isLiteral) {
        leftValue = result.getValue(leftVar).getLiteralValue.toString
        rightValue = rightVal.getLiteralValue.toString
      } else if (rightVal.isURI) {
        leftValue = result.getValue(leftVar).getURI
        rightValue = rightVal.getURI
      }
    } // Left side is value and right side is variable
    else if (left.isInstanceOf[NodeVal] && right.isInstanceOf[NodeVar]) {
      val leftVal = left.asInstanceOf[NodeVal].getNode
      val rightVar = right.asInstanceOf[NodeVar].getNode
      if (leftVal.isLiteral) {
        leftValue = leftVal.getLiteralValue.toString
        rightValue = result.getValue(rightVar).getLiteralValue.toString
      } else if (leftVal.isURI) {
        leftValue = leftVal.getURI
        rightValue = result.getValue(rightVar).getURI
      }
    } // Left side is variable and right side is function
    else if (left.isInstanceOf[NodeVar] && right.isInstanceOf[Function]) {
      val leftVar = left.asInstanceOf[NodeVar].getNode
      val rightVal = right.asInstanceOf[Function].getValue(result)
      if (rightVal.isLiteral) {
        leftValue = result.getValue(leftVar).getLiteralValue.toString
        rightValue = rightVal.getLiteralValue.toString
      } else if (rightVal.isURI) {
        leftValue = result.getValue(leftVar).getURI
        rightValue = rightVal.getURI
      }
    }

    /*val leftNodeValue = NodeValue.makeDecimal(leftValue.toDouble)
    val rightNodeValue = NodeValue.makeDecimal(rightValue.toDouble)
    NodeValue.compare(leftNodeValue, rightNodeValue)*/

    leftValue.compare(rightValue)
  }

  def getLeftExpr: Expression = { left }

  def getRightNode: Expression = { right }

  override def toString: String = { "Left Expression: " + left.toString + "; Right Expression: " + right.toString }
}
