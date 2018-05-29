package net.sansa_stack.query.spark.graph.jena.expression
import net.sansa_stack.query.spark.graph.jena.model.SparkExecutionModel
import net.sansa_stack.query.spark.graph.jena.util.{BasicGraphPattern, Result}
import org.apache.jena.graph.Node
import org.apache.jena.graph.Triple

class Exists(bgp: BasicGraphPattern) extends Filter {

  private val tag = "Exists"

  private val pattern = SparkExecutionModel.basicGraphPatternMatch(bgp)

  override def evaluate(result: Map[Node, Node]): Boolean = {
    throw new UnsupportedOperationException
  }

  override def evaluate(result: Result[Node]): Boolean = {
    // compiler here
    true
  }

  override def getTag: String = { tag }

}
