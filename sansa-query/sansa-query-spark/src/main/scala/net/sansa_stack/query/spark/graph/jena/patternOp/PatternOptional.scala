package net.sansa_stack.query.spark.graph.jena.patternOp

import net.sansa_stack.query.spark.graph.jena.ExprParser
import net.sansa_stack.query.spark.graph.jena.expression.Filter
import net.sansa_stack.query.spark.graph.jena.model.{IntermediateResult, SparkExecutionModel}
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.Op
import org.apache.jena.sparql.algebra.op.OpLeftJoin
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._


/**
 * Class that execute SPARQL OPTIONAL operation
 */
class PatternOptional(op: OpLeftJoin) extends PatternOp {

  private val tag = "OPTIONAL"
  private val id = op.hashCode()

  @deprecated("this method will be removed", "")
  override def execute(
    input: Array[Map[Node, Node]],
    graph: Graph[Node, Node],
    session: SparkSession): Array[Map[Node, Node]] = {
    // compiler here
    input
  }

  override def execute(): Unit = {
    val leftId = op.getLeft.hashCode()
    val rightId = op.getRight.hashCode()
    val leftResult = IntermediateResult.getResult(leftId).cache()
    var rightResult = IntermediateResult.getResult(rightId).cache()

    if (op.getExprs != null) {
      val filters = op.getExprs.getList.asScala.toList.map { expr =>
        new ExprParser(expr).getExpression match {
          case e: Filter => e
          case _ => throw new UnsupportedOperationException
        }
      }
      rightResult = SparkExecutionModel.filter(rightResult, filters)
    }

    val newResult = SparkExecutionModel.leftJoin(leftResult, rightResult)
    IntermediateResult.putResult(id, newResult)
    IntermediateResult.removeResult(leftId)
    IntermediateResult.removeResult(rightId)
  }

  def getOp: Op = { op }

  override def getId: Int = { id }

  override def getTag: String = { tag }
}
