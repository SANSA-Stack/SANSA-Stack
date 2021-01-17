package net.sansa_stack.query.spark.graph.jena.resultOp

import net.sansa_stack.query.spark.graph.jena.Ops
import org.apache.jena.graph.Node

/**
  * Trait for all operations related to deal with solution mapping directly.
  *
  * @author Zhe Wang
  */
trait ResultOp extends Ops {

  def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]]

  override def getTag: String
}
