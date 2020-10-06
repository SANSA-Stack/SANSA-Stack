package net.sansa_stack.query.spark.graph.jena.patternOp

import net.sansa_stack.query.spark.graph.jena.Ops
import org.apache.jena.graph.Node
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

/**
  * Trait for all operations related to deal with graph pattern.
  *
  * @author Zhe Wang
  */
trait PatternOp extends Ops {

  def execute(input: Array[Map[Node, Node]],
              graph: Graph[Node, Node],
              session: SparkSession): Array[Map[Node, Node]]

  override def getTag: String
}
