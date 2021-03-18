package net.sansa_stack.inference.rules.plan

import org.apache.jena.reasoner.rulesys.Rule

/**
  * A SQL generator for rules.
  *
  * @author Lorenz Buehmann
  */
trait SQLGenerator {

  /**
    * Generates a SQL query for the given rule.
    *
    * @param rule the rule
    * @return the SQL query
    */
  def generateSQLQuery(rule: Rule): String

}
