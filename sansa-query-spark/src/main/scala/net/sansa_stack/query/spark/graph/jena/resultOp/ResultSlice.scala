package net.sansa_stack.query.spark.graph.jena.resultOp

import net.sansa_stack.query.spark.graph.jena.model.{IntermediateResult, SparkExecutionModel}
import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpSlice
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Class that execute SPARQL LIMIT and OFFSET operations
  * @param op Slice operator
  */
class ResultSlice(op: OpSlice) extends ResultOp {

  private val tag = "LIMIT and OFFSET"
  private val id = op.hashCode()
  private val limit = op.getLength.toInt
  private val offset = op.getStart.toInt

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    if(op.getStart < 0){    // no offset
      input.take(op.getLength.toInt)
    }
    else {
      input.slice(op.getStart.toInt, op.getLength.toInt+op.getStart.toInt)
    }
  }

  override def execute(): Unit = {
    val oldResult = IntermediateResult.getResult(op.getSubOp.hashCode())
    val newResult = SparkExecutionModel.slice(oldResult, limit, offset)
    IntermediateResult.putResult(id, newResult)
    IntermediateResult.removeResult(op.getSubOp.hashCode())
  }

  override def getTag: String = { tag }

  override def getId: Int = { id }
}
