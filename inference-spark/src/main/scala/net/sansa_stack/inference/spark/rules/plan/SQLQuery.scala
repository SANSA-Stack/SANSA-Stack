package net.sansa_stack.inference.spark.rules.plan

import org.apache.jena.graph.{Node, Triple}
import net.sansa_stack.inference.utils.RuleUtils

import scala.collection.mutable

/**
  * @author Lorenz Buehmann
  */
class SQLQuery(triple: Triple) {

  val selectedVars = mutable.Set[Node]()

  def selectableVariables = RuleUtils.varsOf(triple)

  def select(variable: Node) = selectedVars += variable




}
