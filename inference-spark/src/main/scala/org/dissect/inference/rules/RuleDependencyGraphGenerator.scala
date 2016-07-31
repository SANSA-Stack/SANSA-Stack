package org.dissect.inference.rules

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.vocabulary.RDFS
import org.dissect.inference.utils.RuleUtils
import org.dissect.inference.utils.RuleUtils._

import scala.language.{existentials, implicitConversions}
import scalax.collection.GraphPredef._

/**
  * A generator for a so-called dependency graph based on a given set of rules.
  * A dependency graph is a directed graph representing dependencies of several objects towards each other.
  * It is possible to derive an evaluation order or the absence of an evaluation order that respects the given
  * dependencies from the dependency graph.
  *
  * @author Lorenz Buehmann
  */
object RuleDependencyGraphGenerator {

  /**
    * Generates the rule dependency graph for a given set of rules.
    *
    * @param rules the set of rules
    * @param f a function that denotes whether a rule r1 depends on another rule r2
    * @return the rule dependency graph
    */
  def generate(rules: Set[Rule], f:(Rule, Rule) => Boolean = dependsOn): RuleDependencyGraph = {
    // create empty graph
    val g = new RuleDependencyGraph()

    rules.foreach(r => g.add(r))

    // add edge for each rule r1 that depends on another rule r2
    for (r1 <- rules; r2 <- rules) {
      if (f(r1, r2)) // r1 depends on r2
        g += r1 ~> r2
      else if (f(r2, r1)) // r2 depends on r1
        g += r2 ~> r1
      else if (f(r1, r1)) // r1 depends on r1, i.e. reflexive dependency
        g += r1 ~> r1
    }

    g
  }

  /**
    * Checks whether rule `rule1` depends on rule `rule2`.
    * This methods currently checks if there is a triple pattern in the head of `rule2` that also occurs in the
    * body of `rule1`.
    *
    * @param rule1 the first rule
    * @param rule2 the second rule
    * @return whether the first rule depends on the second rule
    */
  def dependsOn(rule1: Rule, rule2: Rule) : Boolean = {
    // head of rule2
    val head2TriplePatterns = rule2.headTriplePatterns()
    // body of rule1
    val body1TriplePatterns = rule1.bodyTriplePatterns()

    var ret = false

    for (tp2 <- head2TriplePatterns; tp1 <- body1TriplePatterns) {
      if (tp2.getPredicate.equals(tp1.getPredicate)) { // matching predicates
        ret = true
      } else {
        if(tp1.getPredicate.isVariable && tp2.getPredicate.equals(RDFS.subPropertyOf.asNode())) {
          ret = true
        }
      }

    }

    ret
  }
}
