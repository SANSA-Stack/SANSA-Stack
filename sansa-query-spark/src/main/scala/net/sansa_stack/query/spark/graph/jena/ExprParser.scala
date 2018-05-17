package net.sansa_stack.query.spark.graph.jena

import net.sansa_stack.query.spark.graph.jena.expression._
import org.apache.jena.sparql.algebra.walker.{ExprVisitorFunction, Walker}
import org.apache.jena.sparql.expr._

import scala.collection.JavaConversions._
import scala.collection.mutable

class ExprParser(expr: Expr) extends ExprVisitorFunction with Serializable {

  private val filter = new mutable.Queue[ExprFilter]()
  private val function = new mutable.Queue[ExprFunc]()

  Walker.walk(expr, this)

  override def visitExprFunction(func: ExprFunction): Unit = {
    println(func+":ExprFunction")
  }

  override def visit(func: ExprFunction0): Unit = {
    println(func+":ExprFunction0")
  }

  override def visit(func: ExprFunction1): Unit = {
    println(func+":ExprFunction1")
    func match {
      case _: E_Bound => filter += new ExprBound(func.getArg.asVar.asNode)
      case _: E_LogicalNot => filter.last.asInstanceOf[ExprBound].setLogic(false)
      case _: E_Lang =>
      case e: E_Str => println(e+":E_Str")
    }
  }

  override def visit(func: ExprFunction2): Unit = {
    println(func+":ExprFunction2")
    func match {
      case e: E_Equals => filter += new ExprCompare(e)
      case e: E_NotEquals => filter += new ExprCompare(e)
      case e: E_GreaterThan => filter += new ExprCompare(e)
      case e: E_GreaterThanOrEqual => filter += new ExprCompare(e)
      case e: E_LessThan => filter += new ExprCompare(e)
      case e: E_LessThanOrEqual => filter += new ExprCompare(e)
      case _: E_Add =>
      case _: E_Subtract =>
      case _: E_LogicalAnd => val left = filter.dequeue()
        val right = filter.dequeue()
        filter += new ExprLogical(left, right, "And")
      case _: E_LogicalOr => val left = filter.dequeue()
        val right = filter.dequeue()
        filter += new ExprLogical(left, right, "Or")
      case e: E_LangMatches => filter += new ExprLangMatches(e)
      case _ =>
        throw new UnsupportedOperationException("Not support the expression of ExprFunction2")
    }
  }

  override def visit(func: ExprFunction3): Unit = {
    println(func+":ExprFunction3")
  }

  override def visit(func: ExprFunctionN): Unit = {
    println(func+":ExprFunctionN")
    func match {
      case _: E_Regex => val left = func.getArgs.toList.head.asVar.asNode
        val right = func.getArgs.toList(1).getConstant.asNode
        filter += new ExprRegex(left, right)
      case e: E_BNode => println(e+":E_BNode")
      case e: E_Call => println(e+":E_Call")
      case e: E_Coalesce => println(e+":E_Coalesce")
      case e: E_Function => function.last.setFunction(e)
      case _ =>  throw new UnsupportedOperationException("Not support the expression of ExprFunctionN")
    }
  }

  override def visit(exprFunctionOp: ExprFunctionOp): Unit = {
    println(exprFunctionOp+":ExprFunctionOp")
    exprFunctionOp match {
      case e: E_Exists => println(e.getElement)
    }
  }

  override def visit(exprAggregator: ExprAggregator): Unit = {
    println(exprAggregator+":ExprAggregator")
  }

  override def visit(exprNone: ExprNone): Unit = {
    println(exprNone+":ExprNone")
  }

  override def visit(exprVar: ExprVar): Unit = {
    println(exprVar+":ExprVar")
    function += new ExprFunc(exprVar)
  }

  override def visit(nodeValue: NodeValue): Unit = {
    println(nodeValue+":NodeValue")
  }

  def getFilter: ExprFilter = {
    filter.last
  }

  def getFunction: ExprFunc = {
    function.last
  }
}
