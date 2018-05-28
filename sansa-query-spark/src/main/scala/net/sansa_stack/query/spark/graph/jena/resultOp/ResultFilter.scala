package net.sansa_stack.query.spark.graph.jena.resultOp

import net.sansa_stack.query.spark.graph.jena.ExprParser
import net.sansa_stack.query.spark.graph.jena.expression.Filter
import net.sansa_stack.query.spark.graph.jena.model.{IntermediateResult, SparkExecutionModel}
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpFilter
import org.apache.jena.sparql.expr.ExprList

import scala.collection.JavaConversions._

/**
  * Class that execute SPARQL FILTER operation
  */
class ResultFilter(op: OpFilter) extends ResultOp {

  private val tag = "FILTER"
  private val id = op.hashCode
  private val filters = op.getExprs.getList.toList.map{ expr =>
    new ExprParser(expr).getExpression match{
      case e:Filter => e
      case _ => throw new UnsupportedOperationException
    }
  }

  /**
    * Filter the result by the given filter expression
    * @param input solution mapping to be filtered
    * @return solution mapping after filtering
    */
  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    val expr = op.getExprs.getList.toList.head
    val exprParser = new ExprParser(expr)
    val filterOp = exprParser.getFilter
    var intermediate = input
    //filter.foreach(expr => intermediate = intermediate.filter(solution => expr.evaluate(solution)))
    intermediate = intermediate.filter(solution => filterOp.evaluate(solution))
    val output = intermediate
    output
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
