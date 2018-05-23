package net.sansa_stack.query.spark.graph.jena.patternOp

import net.sansa_stack.query.spark.graph.jena.Ops
import net.sansa_stack.query.spark.graph.jena.resultOp.ResultOp
import net.sansa_stack.query.spark.graph.jena.util.{BasicGraphPattern, ResultMapping}
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.sparql.algebra.Op
import org.apache.jena.sparql.algebra.op.OpUnion
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Class that execute SPARQL UNION operations
  */
class PatternUnion(op: OpUnion) extends PatternOp {

  private val tag = "UNION"

  // Deprecated
  override def execute(input: Array[Map[Node, Node]],
                       graph: Graph[Node, Node],
                       session: SparkSession): Array[Map[Node, Node]] = {
    // compiler here
    input
  }

  override def execute(): Unit = {
    // compiler here
  }

  def getOp: Op = { op }

  override def getTag: String = { tag }
}
