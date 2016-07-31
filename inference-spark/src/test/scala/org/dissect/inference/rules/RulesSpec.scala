package org.dissect.inference.rules

import org.dissect.inference.UnitSpec
import org.dissect.inference.utils.RuleUtils
import org.dissect.inference.utils.RuleUtils.RuleExtension

/**
  * Test class for rules.
  *
  * @author Lorenz Buehmann
  */
class RulesSpec extends UnitSpec {

  behavior of "operations on rules"

  val rules = RuleUtils.load("test.rules")

  "rule 'prp-trp'" should "be cyclic" in {
    assert(RuleUtils.isCyclic(RuleUtils.byName(rules, "prp-trp").get) == true)
  }

  "rule 'prp-symp'" should "not be cyclic" in {
    assert(RuleUtils.isCyclic(RuleUtils.byName(rules, "prp-symp").get) == false)
  }

  "rule 'rdfs11'" should "be cyclic" in {
    assert(RuleUtils.isCyclic(RuleUtils.byName(rules, "rdfs11").get) == true)
  }

  "rule 'rdfs2'" should "not be cyclic" in {
    assert(RuleUtils.isCyclic(RuleUtils.byName(rules, "rdfs2").get) == false)
  }

  "rdfp5a and rdfp5b " should "have same body" in {
    val r1 = RuleUtils.byName(rules, "rdfp5a").get
    val r2 = RuleUtils.byName(rules, "rdfp5b").get
    assert(r1.sameBody(r2) == true)
  }

  "rdfp8ax and rdfp8bx " should "not have same body" in {
    val r1 = RuleUtils.byName(rules, "rdfp8ax").get
    val r2 = RuleUtils.byName(rules, "rdfp8bx").get
    assert(r1.sameBody(r2) == false)
  }

  "rdfp14bx and rdfp14a " should "be merged" in {
    val r1 = RuleUtils.byName(rules, "rdfp14bx").get
    val r2 = RuleUtils.byName(rules, "rdfp14a").get
    assert(RuleUtils.canMerge(r1, r2) == true)
  }

}
