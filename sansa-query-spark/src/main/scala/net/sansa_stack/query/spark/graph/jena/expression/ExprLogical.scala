package net.sansa_stack.query.spark.graph.jena.expression

import org.apache.jena.graph.Node

class ExprLogical(left: ExprFilter, right: ExprFilter, op: String) extends ExprFilter {

  private val tag = "Filter Logical"

  override def evaluate(solution: Map[Node, Node]): Boolean = {
    if(op.equals("And")){
      left.evaluate(solution) && right.evaluate(solution)
    }
    else if(op.equals("Or")){
      left.evaluate(solution) || right.evaluate(solution)
    }
    else {
      false
    }
  }

  override def getTag: String = tag
}
