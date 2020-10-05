package net.sansa_stack.query.spark.dof.node

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

class SPO[N: ClassTag](spo: Seq[RDD[N]]) extends java.io.Serializable {

  private val predicates = NodeIndexed(spo(0))
  private val subjects = NodeIndexed(spo(1))
  private val objects = NodeIndexed(spo(2))

  def getSubject: NodeIndexed[N] = subjects
  def getPredicate: NodeIndexed[N] = predicates
  def getObject: NodeIndexed[N] = objects
}
