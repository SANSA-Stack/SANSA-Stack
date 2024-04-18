package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node

class LangMatches(left: Expression, right: Expression) extends FilterTwo(left, right) {

  private val tag = "Lang Matches"
  private val leftFunc = left match {
    case e: Lang => e
    case _ => throw new TypeNotPresentException("Lang", new Throwable)
  }
  private val rightVal = right match {
    case e: NodeVal => e.getNode
    case _ => throw new TypeNotPresentException("Node Value", new Throwable)
  }

  override def evaluate(result: Map[Node, Node]): Boolean = {
    // compiler here
    throw new UnsupportedOperationException
  }

  override def evaluate(result: Result[Node]): Boolean = {
    val leftValue = leftFunc.getValue(result)
    val leftLang = leftValue.toString().split("@")
    val rightLang = rightVal.getLiteralLexicalForm
    if (rightLang.toString().equals("*")) { // lang pattern is *, all literal has lang is true
      if (leftLang.lengthCompare(2) == 0) {
        true
      } else {
        false
      }
    } else {
      if (leftLang.lengthCompare(2) == 0) {
        leftLang(1).toLowerCase.contains(rightLang.toLowerCase)
      } else {
        false
      }
    }
  }

  override def getTag: String = { tag }
}
