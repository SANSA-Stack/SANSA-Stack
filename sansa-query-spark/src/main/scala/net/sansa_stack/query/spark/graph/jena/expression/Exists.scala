package net.sansa_stack.query.spark.graph.jena.expression

import net.sansa_stack.query.spark.graph.jena.model.SparkExecutionModel
import net.sansa_stack.query.spark.graph.jena.util.{BasicGraphPattern, Result}
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD

class Exists(bgp: BasicGraphPattern) extends Pattern {

  private val tag = "Exists"

  private val pattern = SparkExecutionModel.basicGraphPatternMatch(bgp)

  override def evaluate(result: RDD[Result[Node]]): RDD[Result[Node]] = {
    val join = SparkExecutionModel.leftJoin(result, pattern)
    val varSize = join.map(r=>r.getField.size).reduce((i,j)=>math.max(i,j))
    join.filter(r=>r.getField.size == varSize)
  }

  override def getTag: String = { tag }

  def getPattern: RDD[Result[Node]] ={ pattern }
}
