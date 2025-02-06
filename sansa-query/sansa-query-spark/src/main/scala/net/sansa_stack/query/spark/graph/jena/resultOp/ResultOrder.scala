package net.sansa_stack.query.spark.graph.jena.resultOp

import net.sansa_stack.query.spark.graph.jena.model.IntermediateResult
import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpOrder
import org.apache.spark.rdd.RDD

import scala.jdk.CollectionConverters._
import scala.reflect._

/**
 * Class that execute SPARQL ORDER BY operation.
 * Currently support for ordering by at most four variables in the same time.
 */
class ResultOrder(op: OpOrder) extends ResultOp {

  private val tag = "ORDER BY"
  private val id = op.hashCode()

  @deprecated("this method will be removed", "")
  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    // val vars = op.getConditions.toList.map(sc => new ExprParser(sc.getExpression).getVar.getAsNode)
    throw new UnsupportedOperationException
  }

  override def execute(): Unit = {
    val conditions = op.getConditions.asScala.toList // .map(sc => new ExprParser(sc.getExpression))
    val vars = conditions.map(condition => condition.expression.asVar)
    val dirs = conditions.map(condition => condition.direction)
    val oldResult = IntermediateResult.getResult(op.getSubOp.hashCode()).cache()
    var newResult: RDD[Result[Node]] = null
    conditions.length match {
      case 1 => newResult = oldResult.sortBy(result =>
        result.getValue(vars.head).toString())(order1(dirs.head), classTag[String])
      case 2 => newResult = oldResult.sortBy(result =>
        (result.getValue(vars.head).toString(), result.getValue(vars(1)).toString()))(
        order2(dirs.head, dirs(1)),
        classTag[(String, String)])
      case 3 => newResult = oldResult.sortBy(result =>
        (result.getValue(vars.head).toString(), result.getValue(vars(1)).toString(),
          result.getValue(vars(2)).toString()))(order3(dirs.head, dirs(1), dirs(2)), classTag[(String, String, String)])
      case 4 => newResult = oldResult.sortBy(result =>
        (result.getValue(vars.head).toString(), result.getValue(vars(1)).toString(),
          result.getValue(vars(2)).toString(), result.getValue(vars(3)).toString()))(
        order4(dirs.head, dirs(1), dirs(2), dirs(3)),
        classTag[(String, String, String, String)])
    }
    newResult.cache()
    IntermediateResult.putResult(id, newResult)
    IntermediateResult.removeResult(op.getSubOp.hashCode())
  }

  override def getTag: String = { tag }

  override def getId: Int = { id }

  private def order1(a: Int): Ordering[String] = {
    orderDec(a)
  }

  private def order2(a: Int, b: Int): Ordering[(String, String)] = {
    Ordering.Tuple2(orderDec(a), orderDec(b))
  }

  private def order3(a: Int, b: Int, c: Int): Ordering[(String, String, String)] = {
    Ordering.Tuple3(orderDec(a), orderDec(b), orderDec(c))
  }

  private def order4(a: Int, b: Int, c: Int, d: Int): Ordering[(String, String, String, String)] = {
    Ordering.Tuple4(orderDec(a), orderDec(b), orderDec(c), orderDec(d))
  }

  private def orderDec(direction: Int): Ordering[String] = {
    direction match {
      case -2 => Ordering.String
      case -1 => Ordering.String.reverse
    }
  }
}
