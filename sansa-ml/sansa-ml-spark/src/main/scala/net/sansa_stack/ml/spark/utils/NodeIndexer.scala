package net.sansa_stack.ml.spark.utils

import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.sql.DataFrame

abstract class NodeIndexer {
  protected var vocabulary: Map[Node, Int]

  val inputCols: Array[String] = Array("s", "p", "o")



  def fit(df: DataFrame): NodeIndexerModel = {

    // val allNodes = df.select(inputCols)
    // println(allNodes)

    new NodeIndexerModel
  }
}

class NodeIndexerModel {
  def transform(df: DataFrame): DataFrame = {
    df
  }
}
