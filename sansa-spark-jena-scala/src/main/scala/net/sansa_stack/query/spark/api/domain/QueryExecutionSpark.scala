package net.sansa_stack.query.spark.api.domain

import org.apache.jena.query.QueryExecution
import org.apache.jena.sparql.core.Quad
import org.apache.spark.rdd.RDD

trait QueryExecutionSpark extends QueryExecution {
  def execSelectSpark(): ResultSetSpark
  def execConstructSpark(): RDD[org.apache.jena.graph.Triple]
  def execConstructQuadsSpark(): RDD[Quad]
}
