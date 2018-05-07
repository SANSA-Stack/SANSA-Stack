package net.sansa_stack.query.spark.graph.jena.resultOp

import org.apache.jena.graph.Node

class ResultDistinct extends ResultOp {

  private val tag = "DISTINCT"

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    input.distinct
  }

  override def getTag: String = { tag }
}
