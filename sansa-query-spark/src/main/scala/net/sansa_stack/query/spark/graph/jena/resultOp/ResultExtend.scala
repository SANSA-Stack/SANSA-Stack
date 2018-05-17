package net.sansa_stack.query.spark.graph.jena.resultOp

import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpExtend

import scala.collection.JavaConversions._

class ResultExtend(op: OpExtend) extends ResultOp {

  private val tag = "EXTEND"
  private val sub = op.getVarExprList.getVars.toList.head
  private val exp = op.getVarExprList.getExpr(sub)

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    input.map( mapping =>
      mapping.updated(sub.asNode(), mapping(exp.asVar().asNode())).-(exp.asVar().asNode()))
  }

  override def getTag: String = { tag }
}
