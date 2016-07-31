package org.dissect.inference.rules.plan

import org.apache.jena.graph.{Node, Triple}
import org.dissect.inference.utils.RuleUtils

import scala.collection.mutable

/**
  * @author Lorenz Buehmann
  */
class SQLQuery(triple: Triple) {

  val selectedVars = mutable.Set[Node]()

  def selectableVariables = RuleUtils.varsOf(triple)

  def select(variable: Node) = selectedVars += variable




}
