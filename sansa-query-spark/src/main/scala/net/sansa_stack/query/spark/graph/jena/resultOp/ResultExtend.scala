package net.sansa_stack.query.spark.graph.jena.resultOp

import net.sansa_stack.query.spark.graph.jena.model.{ IntermediateResult, SparkExecutionModel }
import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.Op
import org.apache.jena.sparql.algebra.op.OpExtend
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

class ResultExtend(op: OpExtend) extends ResultOp {

  private val tag = "EXTEND"
  private val id = op.hashCode()
  private val sub = op.getVarExprList.getVars.toList.head
  private val exp = op.getVarExprList.getExpr(sub)

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    input.map(mapping =>
      mapping.updated(sub.asNode(), mapping(exp.asVar().asNode())).-(exp.asVar().asNode()))
  }

  override def execute(): Unit = {
    val subVar = sub.asNode
    val exprVar = exp.asVar.asNode
    val oldResult = IntermediateResult.getResult(op.getSubOp.hashCode()).cache()
    val newResult = SparkExecutionModel.extend(oldResult, subVar, exprVar)
    IntermediateResult.putResult(id, newResult)
    IntermediateResult.removeResult(op.getSubOp.hashCode())
  }

  override def getTag: String = { tag }

  override def getId: Int = { id }

  def getOp: Op = { op }
}
