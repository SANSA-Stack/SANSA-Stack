package net.sansa_stack.inference.rules.minimizer

import net.sansa_stack.inference.rules.RuleDependencyGraph
import net.sansa_stack.inference.utils.Logging

/**
  * A minimization rule.
  *
  * @author Lorenz Buehmann
  */
abstract class MinimizationRule extends Logging {

  /** Name for this rule, automatically inferred based on class name. */
  val ruleName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def apply(graph: RuleDependencyGraph): RuleDependencyGraph

}