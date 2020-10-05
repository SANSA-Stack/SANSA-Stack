package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node

trait Function extends Expression{

  def getValue(result: Map[Node, Node]): Node

  def getValue(result: Result[Node]): Node
}
