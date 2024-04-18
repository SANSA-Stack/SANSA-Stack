package net.sansa_stack.query.spark.graph.jena.resultOp

import net.sansa_stack.query.spark.graph.jena.model.{IntermediateResult, SparkExecutionModel}
import net.sansa_stack.query.spark.graph.jena.util.{Result, ResultFactory}
import org.apache.jena.graph.{Node, NodeFactory}
import org.apache.jena.sparql.algebra.Op
import org.apache.jena.sparql.algebra.op.OpGroup
import org.apache.jena.sparql.expr.ExprAggregator

import scala.collection.JavaConverters._

/**
 * Class that execute SPARQL GROUP BY operation.
 * @param op Group By operator
 */
class ResultGroup(op: OpGroup) extends ResultOp {

  private val tag = "GROUP BY"
  private val id = op.hashCode()

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    val vars = op.getGroupVars.getVars.asScala.toList // e.g. List(?user)
    val aggregates = op.getAggregators.asScala.toList // e.g. List((AGG ?.0 AVG(?age)), (AGG ?.1 MAX(?age)), (AGG ?.2 MIN(?age)))
    var intermediate = input
    vars.length match {
      case 1 =>
        aggregates.foreach { aggr =>
          input.groupBy(map => map(vars.head)).foreach {
            case (node, array) =>
              intermediate = intermediate.map { mapping =>
                if (mapping(vars.head).equals(node)) {
                  val c = mapping ++ ResultGroup.aggregateOp(array, aggr)
                  c
                } else { mapping }
              }
          }
        }
    }
    val output = intermediate
    output
  }

  override def execute(): Unit = {
    val vars = op.getGroupVars.getVars.asScala.toList.map(v => v) // List of variables, e.g. List(?user)
    val aggregates = op.getAggregators.asScala.toList
    val oldResult = IntermediateResult.getResult(op.getSubOp.hashCode()).cache()
    val newResult = SparkExecutionModel.group(oldResult, vars, aggregates)
    IntermediateResult.putResult(id, newResult)
    IntermediateResult.removeResult(op.getSubOp.hashCode())
  }

  override def getTag: String = { tag }

  override def getId: Int = { id }

  def getOp: Op = { op }

}

object ResultGroup {

  def aggregateOp(input: Array[Map[Node, Node]], aggr: ExprAggregator): Map[Node, Node] = {
    val key = aggr.getAggregator.getExprList.asScala.head.getExprVar.getAsNode // e.g. ?age
    val seq = input.map(_(key).getLiteralValue.toString.toDouble)
    val result = seq.aggregate((0.0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    if (aggr.getAggregator.key().contains("sum")) { // e.g. (sum, ?age)
      Map(aggr.getVar -> NodeFactory.createLiteral(result._1.toString))
    } else if (aggr.getAggregator.key().contains("avg")) { // e.g. (avg, ?age)
      Map(aggr.getVar -> NodeFactory.createLiteral((result._1 / result._2).toString))
    } else if (aggr.getAggregator.key().contains("count")) { // e.g. (count, ?age)
      Map(aggr.getVar -> NodeFactory.createLiteral(result._2.toString))
    } else if (aggr.getAggregator.key().contains("max")) { // e.g. (max, ?age)
      Map(aggr.getVar -> NodeFactory.createLiteral(seq.max.toString))
    } else if (aggr.getAggregator.key().contains("min")) { // e.g. (min, ?age)
      Map(aggr.getVar -> NodeFactory.createLiteral(seq.min.toString))
    } else {
      Map(aggr.getVar -> NodeFactory.createBlankNode())
    }
  }

  /**
   * Evaluating the Aggregate Function over the grouping results
   * @param input an Iterable of result grouped
   * @param variable variable to be aliased
   * @param aggrOp set function to evaluate(AVG, SUM, ...)
   * @param key variable to be aggregated
   */
  def aggregateOp(input: Iterable[Result[Node]], variable: Node, aggrOp: String, key: Node): Result[Node] = {
    val seq = input.map(_.getValue(key).getLiteralValue.toString.toDouble)
    val sumAndCount = seq.aggregate((0.0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    if (aggrOp.equals("AVG")) {
      ResultFactory.create(Map(variable -> NodeFactory.createLiteral((sumAndCount._1 / sumAndCount._2).toString)))
    } else if (aggrOp.equals("SUM")) {
      ResultFactory.create(Map(variable -> NodeFactory.createLiteral(sumAndCount._1.toString)))
    } else if (aggrOp.equals("COUNT")) {
      ResultFactory.create(Map(variable -> NodeFactory.createLiteral(sumAndCount._2.toString)))
    } else if (aggrOp.equals("MAX")) {
      ResultFactory.create(Map(variable -> NodeFactory.createLiteral(seq.max.toString)))
    } else if (aggrOp.equals("MIN")) {
      ResultFactory.create(Map(variable -> NodeFactory.createLiteral(seq.min.toString)))
    } else {
      ResultFactory.create(Map(variable -> NodeFactory.createBlankNode()))
    }
  }

}
