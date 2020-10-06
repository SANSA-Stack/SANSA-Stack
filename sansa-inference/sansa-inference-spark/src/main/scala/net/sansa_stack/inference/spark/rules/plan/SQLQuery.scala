package net.sansa_stack.inference.spark.rules.plan

import scala.collection.mutable

import org.apache.jena.graph.{Node, Triple}

import net.sansa_stack.inference.utils.RuleUtils

/**
  * @author Lorenz Buehmann
  */
class SQLQuery(triple: Triple) {

  val selectedVars = mutable.Set[Node]()

  def selectableVariables: List[Node] = RuleUtils.varsOf(triple)

  def select(variable: Node): Unit = selectedVars += variable
}
