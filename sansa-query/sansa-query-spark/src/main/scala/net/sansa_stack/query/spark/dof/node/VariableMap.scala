package net.sansa_stack.query.spark.dof.node

import org.apache.jena.graph.Node
import org.apache.jena.sparql.core.Var
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

class VariableMap[N: ClassTag](spark: SparkSession, constraints: Constraints)
      extends Serializable {
  private val empty = null // spark.emptyRDD
  private var _mapV = init
  def mapV: Map[Var, RDD[N]] = _mapV
  def mapV_=(value: VarRDD[N]): Unit = _mapV = value

  def init: Map[Var, RDD[N]] = {
    var map = Map[Var, RDD[N]]()
    constraints.vars.foreach(v => {
      map = map + (v -> empty)
    })
    map
  }

  def update(node: Node, nodes: RDD[N]): Unit = {
    if (node.isVariable) {
      mapV += (node.asInstanceOf[Var] -> nodes)
    }
  }

  def exists(node: Node): Boolean = node.isVariable && mapV.keySet.contains(node.asInstanceOf[Var])

  def isEmpty(nodes: RDD[N]): Boolean = nodes == empty

  def isUnbounded(node: Node): Boolean = exists(node) && isEmpty(mapV(node.asInstanceOf[Var]))

  def get(node: Node): RDD[N] = {
    if (!node.isVariable) {
      throw new IllegalArgumentException("Variable map does not support constants: " + node)
    }

    mapV.get(node.asInstanceOf[Var]) match {
      case Some(value) => value
      case None => throw new IllegalArgumentException("Variable map does not contain variable: " + node.asInstanceOf[Var])
    }
  }
}
