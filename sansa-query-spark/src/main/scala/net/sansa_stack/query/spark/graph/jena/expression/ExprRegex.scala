package net.sansa_stack.query.spark.graph.jena.expression

import org.apache.jena.graph.Node

/**
  * Class that evaluate solution based on expression. Support expression as FILTER regex(?user "tw:user0")
  * @param variable expression of variable
  * @param value expression of value
  */
class ExprRegex(variable: Node, value: Node) extends ExprFilter {

  private val tag = "Filter Regex"

  override def evaluate(solution: Map[Node, Node]): Boolean = {
    if(solution(variable).isLiteral){
      solution(variable).getLiteral.getValue.toString
        .contains(value.getLiteralValue.toString)
    }
    else{
      false
    }
  }

  override def getTag: String = { tag }
}
