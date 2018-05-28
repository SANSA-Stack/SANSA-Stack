package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node

class Logical(left: Filter, right: Filter, op: String) extends Filter {

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

  override def evaluate(solution: Result[Node]): Boolean = {
    true
  }

  override def getTag: String = tag
}
