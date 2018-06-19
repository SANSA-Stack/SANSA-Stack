package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node

abstract class FunctionTwo(protected val left: Expression, protected val right: Expression) extends Function {

  protected def getLeftAndRightValue(result: Result[Node]): (Node, Node) = {
    if(left.isInstanceOf[NodeVar] && right.isInstanceOf[NodeVal]){
      (result.getValue(left.asInstanceOf[NodeVar].getNode), right.asInstanceOf[NodeVal].getNode)
    }
    else if(left.isInstanceOf[NodeVal] && right.isInstanceOf[NodeVar]){
      (left.asInstanceOf[NodeVal].getNode, result.getValue(right.asInstanceOf[NodeVar].getNode))
    }
    else {
      throw new TypeNotPresentException("Variable and Value", new Throwable)
    }
  }

  def getLeftExpr: Expression = { left }

  def getRightNode: Expression = { right }

  override def toString: String = { "Left Expression: "+left.toString+"; Right Expression: "+right.toString }
}
