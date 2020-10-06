package net.sansa_stack.query.spark.graph.jena.patternOp

import net.sansa_stack.query.spark.graph.jena.model.{IntermediateResult, SparkExecutionModel}
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpMinus
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

/**
  * Class that execute SPARQL MINUS
  */
class PatternMinus(op: OpMinus) extends PatternOp {

  private val tag = "MINUS"
  private val id = op.hashCode()

  @deprecated("this method will be removed", "")
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
    val newResult = SparkExecutionModel.minus(leftResult, rightResult)
    IntermediateResult.putResult(id, newResult)
    IntermediateResult.removeResult(leftId)
    IntermediateResult.removeResult(rightId)
  }

  override def getTag: String = { tag }

  override def getId: Int = { id }

}
