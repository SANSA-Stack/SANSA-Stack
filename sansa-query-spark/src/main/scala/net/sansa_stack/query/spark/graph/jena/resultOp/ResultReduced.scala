package net.sansa_stack.query.spark.graph.jena.resultOp

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node
import org.apache.jena.sparql.algebra.op.OpReduced
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Class that execute REDUCED modifier. Support syntax as SELECT REDUCED ?user WHERE ...
  */
class ResultReduced(op: OpReduced) extends ResultOp {

  private val tag = "REDUCED"
  private val id = op.hashCode()

  override def execute(input: Array[Map[Node, Node]]): Array[Map[Node, Node]] = {
    var duplicates = input.groupBy(identity).mapValues(_.length).filter{ case(_, count) =>
      count > 1}.keys.toArray
    input.filter(map =>
      if(duplicates.contains(map)){
        duplicates = duplicates.filterNot(_.equals(map))
        false
      }
      else{ true })
  }

  override def execute(): Unit = {
    // compiler here
  }

  override def getTag: String = { tag }

  override def getId: Int = { id }
}
