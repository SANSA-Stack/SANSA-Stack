package net.sansa_stack.query.spark.graph.jena

import org.apache.jena.query.QueryExecutionFactory

import scala.io.Source

class Construct{

}

object Construct {
  def main(args: Array[String]): Unit = {
    val path = "src/resources/BSBM/query12.txt"
    val queryString = Source.fromFile(path).mkString
    val queryExec = QueryExecutionFactory.create(Source.fromFile(path).mkString)
  }
}
