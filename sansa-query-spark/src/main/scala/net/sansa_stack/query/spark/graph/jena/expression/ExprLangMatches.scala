package net.sansa_stack.query.spark.graph.jena.expression

import org.apache.jena.graph.Node
import org.apache.jena.sparql.expr.nodevalue.NodeFunctions
import org.apache.jena.sparql.expr.{E_Lang, E_LangMatches, Expr, NodeValue}

class ExprLangMatches(e: E_LangMatches) extends ExprFilter {

  private val tag = "Lang Matches"
  private val left = e.getArg1
  private val right = e.getArg2

  override def evaluate(solution: Map[Node, Node]): Boolean = {
    val leftNode = solution(left.asInstanceOf[E_Lang].getArg.asVar.asNode)
    val leftLang = leftNode.toString.split("@")
    val rightLang = right.getConstant.asNode.getLiteralLexicalForm
    if(rightLang.toString.equals("*")) {    // lang pattern is *, all literal has lang is true
      if(leftLang.lengthCompare(2) == 0){
        true
      } else{
        false
      }
    } else {
      if(leftLang.lengthCompare(2) == 0){
        println(leftLang(1).toLowerCase)
        println(rightLang.toString.toLowerCase)
        leftLang(1).toLowerCase.contains(rightLang.toLowerCase)
      } else {
        false
      }
    }
  }

  override def getTag: String = { tag }

  def getLeft: Expr = { left }

  def getRight: Expr = { right }
}
