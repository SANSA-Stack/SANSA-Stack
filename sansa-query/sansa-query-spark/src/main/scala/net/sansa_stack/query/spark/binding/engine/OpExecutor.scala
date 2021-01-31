package net.sansa_stack.query.spark.binding.engine

import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD

/*
 * An OpExecutor for Spark analoguous to [[org.apache.jena.sparql.engine.main.OpExecutor]]
 */
trait OpExecutor {
  def execute(op: OpProject, rdd: RDD[Binding]): RDD[Binding]
  def execute(op: OpGroup, rdd: RDD[Binding]): RDD[Binding]
  def execute(op: OpOrder, rdd: RDD[Binding]): RDD[Binding]
  def execute(op: OpExtend, rdd: RDD[Binding]): RDD[Binding]
  def execute(op: OpService, rdd: RDD[Binding]): RDD[Binding]
  def execute(op: OpUnion, rdd: RDD[Binding]): RDD[Binding]
  def execute(op: OpDistinct, rdd: RDD[Binding]): RDD[Binding]
  def execute(op: OpReduced, rdd: RDD[Binding]): RDD[Binding]
  def execute(op: OpFilter, rdd: RDD[Binding]): RDD[Binding]
  def execute(op: OpSlice, rdd: RDD[Binding]): RDD[Binding]

  /* Definition is not yet complete */
}
