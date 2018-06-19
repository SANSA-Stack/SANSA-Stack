package net.sansa_stack.query.spark.graph.jena.expression

abstract class FunctionOne(protected val expr: Expression) extends Function {

  def getExpr: Expression = { expr }

  override def toString: String = { "Expression: " + expr.toString }
}
