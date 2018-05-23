package net.sansa_stack.query.spark.graph.jena.patternOp

import net.sansa_stack.query.spark.graph.jena.model.{IntermediateResult, SparkExecutionModel}
import net.sansa_stack.query.spark.graph.jena.resultOp.ResultFilter
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.Op
import org.apache.jena.sparql.algebra.op.OpLeftJoin
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Class that execute SPARQL OPTIONAL operation
  */
class PatternOptional(op: OpLeftJoin) extends PatternOp {

  private val tag = "OPTIONAL"

  // Deprecated
  override def execute(input: Array[Map[Node, Node]],
                       graph: Graph[Node, Node],
                       session: SparkSession): Array[Map[Node, Node]] = {
    // compiler here
    input
  }

  override def execute(): Unit = {
    val leftId = op.getLeft.hashCode()
    val rightId = op.getRight.hashCode()
    val leftResult = IntermediateResult.getResult(leftId).cache()
    val rightResult = IntermediateResult.getResult(rightId).cache()
    val newResult = SparkExecutionModel.leftJoin(leftResult, rightResult)
    IntermediateResult.putResult(op.hashCode(), newResult)
    IntermediateResult.removeResult(leftId)
    IntermediateResult.removeResult(rightId)
  }

  def getOp: Op = { op }

  override def getTag: String = { tag }
}
