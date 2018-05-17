package net.sansa_stack.query.spark.graph.jena.resultOp

import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpSlice

/**
  * Class that execute SPARQL LIMIT and OFFSET operations
  * @param op Slice operator
  */
class ResultSlice(op: OpSlice) extends ResultOp {

  private val tag = "LIMIT and OFFSET"

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    if(op.getStart < 0){    // no offset
      input.take(op.getLength.toInt)
    }
    else {
      input.slice(op.getStart.toInt, op.getLength.toInt+op.getStart.toInt)
    }
  }

  override def getTag: String = { tag }
}
