package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node

trait Filter extends Expression {

  def evaluate(result: Map[Node, Node]): Boolean

  def evaluate(result: Result[Node]): Boolean

}
