package net.sansa_stack.query.spark.graph.jena.resultOp

import net.sansa_stack.query.spark.graph.jena.ExprParser
import net.sansa_stack.query.spark.graph.jena.expression.Expression
import net.sansa_stack.query.spark.graph.jena.model._
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpFilter
import org.apache.jena.sparql.expr.ExprList

import scala.jdk.CollectionConverters._


/**
 * Class that execute SPARQL FILTER operation
 */
class ResultFilter(op: OpFilter) extends ResultOp {

  private val tag = "FILTER"
  private val id = op.hashCode
  private val filters = op.getExprs.getList.asScala.toList.map { expr =>
    new ExprParser(expr).getExpression match {
      case e: Expression => e
      case _ => throw new UnsupportedOperationException
    }
  }

  @deprecated("this method will be removed", "")
  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    throw new UnsupportedOperationException
  }

  /**
   * Filter the RDD result and put the new result into intermediate model.
   */
  override def execute(): Unit = {
    val oldResult = IntermediateResult.getResult(op.getSubOp.hashCode())
    val newResult = SparkExecutionModel.filter(oldResult, filters)
    IntermediateResult.putResult(id, newResult)
    IntermediateResult.removeResult(op.getSubOp.hashCode())
  }

  override def getTag: String = { tag }

  override def getId: Int = { id }

  def getExprList: ExprList = { op.getExprs }

}
