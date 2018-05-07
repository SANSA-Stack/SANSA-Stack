package net.sansa_stack.query.spark.graph.jena.expression

import org.apache.jena.graph.Node

/**
  * Class that manipulate expression with bound and unbound operators.
  * @param variable bounded or unbounded variable.
  */
class ExprBound(variable: Node) extends ExprFilter {

  private val tag = "Filter Bound"
  private var logic = true

  override def evaluate(solution: Map[Node, Node]): Boolean = {
    if(solution.keySet.contains(variable)){
      logic
    }
    else {
      !logic
    }
  }

  override def getTag: String = { tag }

  def setLogic(logic: Boolean): Unit = {
    this.logic = logic
  }
}
