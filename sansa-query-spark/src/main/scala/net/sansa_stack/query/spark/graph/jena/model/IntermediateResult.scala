package net.sansa_stack.query.spark.graph.jena.model

import net.sansa_stack.query.spark.graph.jena.util.Result
import org.apache.jena.graph.Node
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class IntermediateResult {

}

object IntermediateResult {

  private val results = mutable.HashMap.empty[Int, RDD[Result[Node]]]

  def putResult(id: Int, result: RDD[Result[Node]]): Unit = {
    results.put(id, result)
  }

  def getResult(id: Int): RDD[Result[Node]] = {
    results(id)
  }

  def removeResult(id: Int): Unit = {
    results.remove(id)
  }

  def returnResultsSize: Int = {
    results.size
  }

  def getFinalResult: RDD[Result[Node]] = {
    if(results.size == 1) {
      results.head._2
    }
    else {
      //throw new UnsupportedOperationException("No final result found")
      results.head._2
    }
  }
}
