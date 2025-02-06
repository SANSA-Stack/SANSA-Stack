package net.sansa_stack.query.spark.graph.jena.patternOp

import net.sansa_stack.query.spark.graph.jena.model.{IntermediateResult, SparkExecutionModel}
import net.sansa_stack.query.spark.graph.jena.util.BasicGraphPattern
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.Op
import org.apache.jena.sparql.algebra.op.OpBGP
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._

/**
 * Class for bgp match with target rdf graph.
 */
class PatternBgp(op: OpBGP) extends PatternOp {

  private val tag = "Bgp Match"
  private val id = op.hashCode
  private val triples = op.getPattern.asScala.toIterator

  override def execute(
    input: Array[Map[Node, Node]],
    graph: Graph[Node, Node],
    session: SparkSession): Array[Map[Node, Node]] = {
    input
  }

  override def execute(): Unit = {
    val bgp = new BasicGraphPattern(triples)
    val result = SparkExecutionModel.basicGraphPatternMatch(bgp)
    IntermediateResult.putResult(id, result)
  }

  def getOp: Op = { op }

  override def getId: Int = { id }

  override def getTag: String = { tag }

}
