package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD

trait Pattern extends Expression {

  def evaluate(result: RDD[Result[Node]]): RDD[Result[Node]]

}
