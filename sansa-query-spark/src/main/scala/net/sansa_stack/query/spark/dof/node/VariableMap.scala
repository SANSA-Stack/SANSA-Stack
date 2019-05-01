package net.sansa_stack.query.spark.dof.node

import org.apache.jena.graph.Node
import org.apache.jena.sparql.core.Var
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

class VariableMap[N: ClassTag](spark: SparkContext, constraints: Constraints) extends Serializable {
  // if null is OK, remove spark and ClassTag
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

  // map contains ?X as key
  def exists(node: Node): Boolean = node.isVariable && mapV.keySet.contains(node.asInstanceOf[Var])

  // RDD for ?X != null
  def isEmpty(nodes: RDD[N]): Boolean = nodes == empty

  // ?X is variable and is not bounded, i.e. is empty yet
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

  def print: Unit = {
    Helper.log("VariableMap = ")
    mapV.foreach(f => {
      Helper.log(f._1)
      if (!isEmpty(f._2)) f._2.foreach(f => Helper.log(f.toString()))
    })
    Helper.log("===============")
  }
}
