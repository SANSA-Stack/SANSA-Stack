package net.sansa_stack.query.spark.graph.jena.expression

abstract class FilterOne(expr: Expression) extends Filter {

  def getExpr: Expression = { expr }

  override def toString: String = { "Expression: " + expr.toString }
}
