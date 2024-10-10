package net.sansa_stack.query.spark.dof.node

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object NodeIndexed {
  def apply[N: ClassTag](data: RDD[N]): NodeIndexed[N] = new NodeIndexed(data)
}

class NodeIndexed[N: ClassTag](data: RDD[N]) extends java.io.Serializable {
  private val _rdd = data.distinct().zipWithIndex()
  def rdd1: RDD[(N, Long)] = _rdd
  private val _reverse = rdd1.map(ni => (ni._2, ni._1))
  def reverserdd1: RDD[(Long, N)] = _reverse

  private val _rddStr = data.map(ni => ni.toString).distinct().zipWithIndex()
  def rddStr: RDD[(String, Long)] = _rddStr
  private val _reverseStr = rddStr.map(ni => (ni._2, ni._1))
  def reverserddStr: RDD[(Long, String)] = _reverseStr

  def getNode(index: Long): RDD[String] = rddStr.filter(_._2 == index).map(_._1)

  def getIndex(node: N): RDD[Long] = rddStr.filter(_._1 == node).map(_._2)
}
