package net.sansa_stack.query.spark.graph.jena

import net.sansa_stack.query.spark.graph.jena.expression._
import net.sansa_stack.query.spark.graph.jena.util.BasicGraphPattern
import org.apache.jena.sparql.algebra.op.OpBGP
import org.apache.jena.sparql.algebra.walker.{ExprVisitorFunction, Walker}
import org.apache.jena.sparql.expr._

import scala.jdk.CollectionConverters._
import scala.collection.mutable

class ExprParser(expr: Expr) extends ExprVisitorFunction with Serializable {

  private val stack = new mutable.Stack[Expression]

  Walker.walk(expr, this)

  override def visit(tripleTerm: ExprTripleTerm): Unit = {
  }

  override def visitExprFunction(func: ExprFunction): Unit = {
  }

  override def visit(func: ExprFunction0): Unit = {
  }

  override def visit(func: ExprFunction1): Unit = {
    var expr: Expression = null
    if (stack.nonEmpty) {
      expr = stack.pop()
    } else {
      throw new NoSuchElementException
    }
    func match {
      case _: E_Bound => stack.push(new Bound(expr))
      case _: E_LogicalNot => stack.push(new LogicalNot(expr))
      case _: E_Lang => stack.push(new Lang(expr))
      case _: E_Str =>
    }
  }

  override def visit(func: ExprFunction2): Unit = {

    var left: Expression = null
    var right: Expression = null

    if (stack.length >= 2) {
      right = stack.pop()
      left = stack.pop()
    } else {
      throw new NoSuchElementException
    }

    func match {
      case _: E_Equals => stack.push(new Equals(left, right))
      case _: E_NotEquals => stack.push(new NotEquals(left, right))
      case _: E_GreaterThan => stack.push(new GreaterThan(left, right))
      case _: E_GreaterThanOrEqual => stack.push(new GreaterThanOrEqual(left, right))
      case _: E_LessThan => stack.push(new LessThan(left, right))
      case _: E_LessThanOrEqual => stack.push(new LessThanOrEqual(left, right))
      case _: E_Add => stack.push(new Add(left, right))
      case _: E_Subtract => stack.push(new Subtract(left, right))
      case _: E_LogicalAnd => stack.push(new LogicalAnd(left, right))
      case _: E_LogicalOr => stack.push(new LogicalOr(left, right))
      case _: E_LangMatches => stack.push(new LangMatches(left, right))
      case _ => throw new UnsupportedOperationException("Not support the expression of ExprFunction2")
    }
  }

  override def visit(func: ExprFunction3): Unit = {
  }

  override def visit(func: ExprFunctionN): Unit = {
    func match {
      case _ => throw new UnsupportedOperationException("Not support the expression of ExprFunctionN")
    }
  }

  override def visit(exprFunctionOp: ExprFunctionOp): Unit = {
    val triples = exprFunctionOp.getGraphPattern.asInstanceOf[OpBGP].getPattern.asScala.toIterator
    val bgp = new BasicGraphPattern(triples)
    exprFunctionOp match {
      case _: E_Exists => stack.push(new Exists(bgp))
      case _: E_NotExists =>
    }
  }

  override def visit(exprAggregator: ExprAggregator): Unit = {
  }

  override def visit(exprNone: ExprNone): Unit = {
  }

  override def visit(exprVar: ExprVar): Unit = {
    stack.push(new NodeVar(exprVar.getAsNode))
  }

  override def visit(nodeValue: NodeValue): Unit = {
    stack.push(new NodeVal(nodeValue.getNode))
  }

  def getExpression: Expression = {
    stack.pop()
  }
}
