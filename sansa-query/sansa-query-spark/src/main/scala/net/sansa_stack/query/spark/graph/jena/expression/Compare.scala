package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node
import org.apache.jena.sparql.expr._

/**
 * Class that evaluate solution based on expression. Support expression with FILTER operators.
 * @param e Expression of the filter.
 *
 * @author Zhe Wang
 */
class Compare(e: ExprFunction2) extends Filter {

  private val tag = "Filter Comparision"
  private val left = e.getArg1
  private val right = e.getArg2

  override def evaluate(solution: Map[Node, Node]): Boolean = {

    val leftValue = solution(left.asVar())
    val boolean: Boolean = {
      if (right.isConstant) {
        compare(leftValue, right)
      } else if (right.isFunction) {
        right match {
          case e: E_Add => if (e.getArg1.isVariable) {
            compare(leftValue, NodeValue.makeDouble(solution(e.getArg1.asVar).getLiteralValue.toString.toDouble +
              e.getArg2.getConstant.toString.toDouble))
          } else {
            compare(leftValue, NodeValue.makeDouble(e.getArg1.getConstant.toString.toDouble +
              solution(e.getArg2.asVar).getLiteralValue.toString.toDouble))
          }
          case e: E_Subtract => if (e.getArg1.isVariable) {
            compare(leftValue, NodeValue.makeDouble(solution(e.getArg1.asVar).getLiteralValue.toString.toDouble -
              e.getArg2.getConstant.toString.toDouble))
          } else {
            compare(leftValue, NodeValue.makeDouble(e.getArg1.getConstant.toString.toDouble -
              solution(e.getArg2.asVar).getLiteralValue.toString.toDouble))
          }
        }
      } else if (right.isVariable) {
        solution(right.asVar())
        false
      } else { false }
    }

    boolean
  }

  override def evaluate(solution: Result[Node]): Boolean = {
    // compiler here
    true
  }

  override def getTag: String = { tag }

  def getLeft: Expr = { left }

  def getRight: Expr = { right }

  private def compare(leftValue: Node, right: Expr): Boolean = {
    if (right.getConstant.isDate) { // compare date
      e.eval(
        NodeValue.makeDate(leftValue.getLiteralLexicalForm),
        NodeValue.makeDate(right.getConstant.getDateTime)).toString.equals("true")
    } else if (right.getConstant.isInteger) { // compare integer
      e.eval(
        NodeValue.makeInteger(leftValue.getLiteralLexicalForm),
        NodeValue.makeInteger(right.getConstant.getInteger)).toString.equals("true")
    } else if (right.getConstant.isFloat) { // compare float
      e.eval(
        NodeValue.makeFloat(leftValue.getLiteralLexicalForm.toFloat),
        NodeValue.makeFloat(right.getConstant.getFloat)).toString.equals("true")
    } else if (right.getConstant.isDouble) { // compare double
      e.eval(
        NodeValue.makeDouble(leftValue.getLiteralLexicalForm.toDouble),
        NodeValue.makeDouble(right.getConstant.getDouble)).toString.equals("true")
    } else if (right.getConstant.isIRI) { // compare URI
      e.eval(NodeValue.makeNode(leftValue), NodeValue.makeNode(right.getConstant.asNode)).toString.equals("true")
    } else {
      false
    }
  }
}
