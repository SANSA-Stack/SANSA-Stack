package net.sansa_stack.query.spark.graph.jena.resultRddOp

import net.sansa_stack.query.spark.graph.jena.Ops
import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Trait for all operations related to deal with solution mapping directly.
  *
  * @author Zhe Wang
  */
trait ResultRddOp extends Ops{

  def execute(input: RDD[Result[Node]], session: SparkSession): RDD[Result[Node]]

  override def getTag: String
}
