package org.dissect.inference.utils

import org.apache.jena.graph.Node
import org.apache.jena.reasoner.TriplePattern
import org.apache.jena.reasoner.rulesys.Rule
import org.dissect.inference.rules.RuleEntailmentType
import org.dissect.inference.rules.RuleEntailmentType._

import scala.collection.JavaConversions._
import scalax.collection.edge.Implicits._
import scalax.collection.edge.LDiEdge
import scalax.collection.mutable.Graph

/**
  * Utility class for rules.
  *
  * @author Lorenz Buehmann
  */
object RuleUtils {


  /**
    * Checks whether a rule is terminological.
    * <p>
    * An rule is considered as terminological, if and only if it contains
    * only terminological triples in its conclusion.
    * </p>
    * @see org.dissect.inference.utils.TripleUtils#isTerminological
    *
    * @param rule the rule to check
    */
  def isTerminological(rule: Rule) : Boolean = {
    var ret = true

    // check whether there are only terminological triples in head
    rule.getHead
      .collect{case b:TriplePattern => b}
      .foreach(
        tp => if(!TripleUtils.isTerminological(tp.asTriple())) {ret = false}
      )
    ret
  }

  /**
    * Checks whether a rule is assertional.
    * <p>
    * An rule is considered as assertional, if and only if it contains
    * only assertional triples in its premises and conclusion.
    * </p>
    * @see org.dissect.inference.utils.TripleUtils#isAssertional
    *
    * @param rule the rule to check
    */
  def isAssertional(rule: Rule) : Boolean = {
    var ret = true

    // check whether there are only assertional triples in body
    rule.getBody
      .collect{case b:TriplePattern => b}
      .foreach(
        tp => if(!TripleUtils.isAssertional(tp.asTriple())) {ret = false}
      )

    // check whether there are only assertional triples in head
    rule.getHead
      .collect{case b:TriplePattern => b}
      .foreach(
        tp => if(!TripleUtils.isAssertional(tp.asTriple())) {ret = false}
      )
    ret
  }

  /**
    * Checks whether a rule is assertional.
    * <p>
    * An rule is considered as hybrid, if and only if it contains both assertional and terminological triples in its
    * premises and only assertional triples in its conclusion.
    * </p>
    * @see org.dissect.inference.utils.TripleUtils#isAssertional
    *
    * @param rule the rule to check
    */
  def isHybrid(rule: Rule) : Boolean = {
    // check for assertional triple in body
    var assertional = false
    rule.getBody
      .collect{case b:TriplePattern => b}
      .foreach(
        tp => if(TripleUtils.isAssertional(tp.asTriple())) {assertional = true}
      )

    // check for terminological triple in body
    var terminological = false
    rule.getBody
      .collect{case b:TriplePattern => b}
      .foreach(
        tp => if(TripleUtils.isTerminological(tp.asTriple())) {terminological = true}
      )

    val hybridBody = assertional && terminological

    // we stop if body is not hybrid
    if(!hybridBody) {
      return false
    }

    // check if there are only assertional triples in head
    var assertionalHead = true
    rule.getHead
      .collect{case b:TriplePattern => b}
      .foreach(
        tp => if(!TripleUtils.isAssertional(tp.asTriple())) {assertionalHead = false}
      )

    assertionalHead
  }

  /**
    * Returns the type of entailment for the given rule
    * @param rule the rule to analyze
    * @return the entailment type
    */
  def entailmentType(rule: Rule) : RuleEntailmentType = {
    if(isAssertional(rule))
      RuleEntailmentType.ASSERTIONAL
    else if(isTerminological(rule))
      RuleEntailmentType.TERMINOLOGICAL
    else if(isHybrid(rule))
      RuleEntailmentType.HYBRID
    else
      None.get
  }

  /**
    * Returns a graph representation of the triple patterns contained in the body of the rule.
    * @param rule the rule
    * @return the directed labeled graph
    */
  def graphOfBody(rule: Rule) : Graph[Node, LDiEdge] = {
    // create empty graph
    val g = Graph[Node, LDiEdge]()

    // add labeled edge p(s,o) for each triple pattern (s p o) in the body of the rule
    rule.getBody.collect { case b: TriplePattern => b }.foreach(
      tp => g += (tp.getSubject ~+> tp.getObject)(tp.getPredicate)
    )
    g
  }

  /**
    * Returns a graph representation of the triple patterns contained in the head of the rule.
    * @param rule the rule
    * @return the directed labeled graph
    */
  def graphOfHead(rule: Rule) : Graph[Node, LDiEdge] = {
    // create empty graph
    val g = Graph[Node, LDiEdge]()

    // add labeled edge p(s,o) for each triple pattern (s p o) in the head of the rule
    rule.getHead.collect { case b: TriplePattern => b }.foreach(
      tp => g += (tp.getSubject ~+> tp.getObject)(tp.getPredicate)
    )
    g
  }

  /**
    * Returns a graph representation of the triple patterns contained in the rule.
    * @param rule the rule
    * @return the directed labeled graph
    */
  def asGraph(rule: Rule) : Graph[Node, LDiEdge] = {
    // create graph for body
    val bodyGraph = graphOfBody(rule)

    // create graph for head
    val headGraph = graphOfHead(rule)

    // return union
    bodyGraph union headGraph
  }

  /**
    * Checks whether a rule denotes the transitive closure(TC) for a given predicate p, i.e.
    * the rule looks like
    * <code>(?s p ?o1), (?o1, p ?o2) -> (?s p ?o2)</code>
    *
    * @param rule the rule to check
    * @return whether it denotes the TC or not
    */
  def isTransitiveClosure(rule: Rule) : Boolean = {
    // TPs contained in body
    val bodyTriplePatterns = rule.bodyTriplePatterns()

    var isTC = false

    // TODO handle body with more than 2 TPs
    if (bodyTriplePatterns.size == 2) {
      // graph for body
      val bodyGraph = graphOfBody(rule)

      // graph for head
      val headGraph = graphOfHead(rule)

      // get source and target node from head (we currently assume that there is only one edge)
      val edge = headGraph.edges.head
      val source = edge.source
      val target = edge.target

      // get the path in body graph
      val path = (bodyGraph get source) pathTo (bodyGraph get target)

      // check if there is a path  ?s -> ?o2 in body such that there is at least one edge labeled with the same predicate
      isTC = path match {
        case Some(value) => value.edges.filter(_.label.equals(edge.label)).toSeq.nonEmpty
        case None => false
      }
    }

    isTC
  }

  /**
    * Checks whether a rule r1 is simply the opposite of another rule r2, i.e.
    * whether it holds that the head of r1 is the body of r2 and vice versa (modulo variable names).
    * @example [r1: (?s p1 ?o), (?o p1 ?s) -> (?s p2 ?o)] and [r2: (?s p2 ?o) -> (?o p1 ?s)
    *
    *
    * @param rule1 the first rule
    * @param rule2 the second rule
    * @return whether rule1 is the inverse of rule2
    */
  def isInverseOf(rule1: Rule, rule2: Rule) : Boolean = {
    // TPs contained in body
    val bodyTriplePatterns = rule1.bodyTriplePatterns()

    false
  }

  /**
    * Checks whether a rule itself is cyclic. Intuitively, this means to check for triples produced in the conclusion
    * that are used as input in the premise.
    *
    * This is rather tricky, i.e. a naive approach which simply looks e.g. for predicates that occur in both, premise and conclusion
    * is not enough because, e.g. a rule [(?s ?p ?o) -> (?o ?p ?s)] would lead to an infinite loop without producing anything new
    * after one iteration. On the other hand, for rules like [(?s ?p ?o1), (?o1 ?p ?o2) -> (?s ?p ?o2)] it's valid.
    * TODO we do not only have to check for common predicates, but also have to analyze the subjects/objects of the
    * triple patterns.
    *
    * @param rule the rule to check
    * @return whether it's cyclic or not
    */
  def isCyclic(rule: Rule) : Boolean = {
    // get the type of the rule
    val ruleType = entailmentType(rule)

    // predicates contained in body
    val bodyPredicates = rule.bodyTriplePatterns()
      .map(tp => tp.getPredicate).toSet

    // predicates contained in head
    val headPredicates = rule.headTriplePatterns()
      .map(tp => tp.getPredicate).toSet

    // predicates that are contained in body and head
    val intersection = bodyPredicates.intersect(headPredicates)

    ruleType match {
      case TERMINOLOGICAL =>
        // check if there is at least one predicate that occurs in body and head
        val bodyPredicates = rule.getBody
          .collect { case b: TriplePattern => b }
          .map(tp => tp.getPredicate).toSet
        val headPredicates = rule.getHead
          .collect { case b: TriplePattern => b }
          .map(tp => tp.getPredicate).toSet

        bodyPredicates.intersect(headPredicates).nonEmpty
      case ASSERTIONAL =>
        // check if there is at least one predicate that occurs in body and head
        val bodyPredicates = rule.getBody
          .collect { case b: TriplePattern => b }
          .map(tp => tp.getPredicate).toSet
        val headPredicates = rule.getHead
          .collect { case b: TriplePattern => b }
          .map(tp => tp.getPredicate).toSet
        bodyPredicates.intersect(headPredicates).nonEmpty
      case _ =>
        // check if there is at least one predicate that occurs in body and head
        val bodyPredicates = rule.getBody
          .collect { case b: TriplePattern => b }
          .map(tp => tp.getPredicate).toSet
        val headPredicates = rule.getHead
          .collect { case b: TriplePattern => b }
          .map(tp => tp.getPredicate).toSet
        bodyPredicates.intersect(headPredicates).nonEmpty

    }
  }

  /**
    * Load a set of rules from the given file.
    * @param filename the file
    * @return a set of rules
    */
  def load(filename: String): Seq[Rule] = {
    Rule.parseRules(org.apache.jena.reasoner.rulesys.Util.loadRuleParserFromResourceFile(filename)).toSeq
  }

  /**
    * Returns a rule by the given name from a set of rules.
    * @param rules the set of rules
    * @param name the name of the rule
    * @return the rule if exist
    */
  def byName(rules: Seq[Rule], name: String): Option[Rule] = {
    rules.foreach(
      r => if (r.getName.equals(name)) return Some(r)
    )
    None
  }


  /**
    * Returns all variables that occur in the body.
    *
    * @param rule the rule
    * @return the variables
    */
  def varsOfBody(rule: Rule): Set[Node] = {
    (for(tp <- rule.bodyTriplePatterns()) yield varsOf(tp)).flatten.toSet
  }

  /**
    * Returns all variables that occur in the head.
    *
    * @param rule the rule
    * @return the variables
    */
  def varsOfHead(rule: Rule): Set[Node] = {
    (for(tp <- rule.bodyTriplePatterns()) yield varsOf(tp)).flatten.toSet
  }

  /**
    * Returns all variables that occur in the triple pattern.
    *
    * @param tp the triple pattern
    * @return the variables
    */
  def varsOf(tp: TriplePattern): List[Node] = {
    varsOf(tp.asTriple())
  }

  def varsOf(tp: org.apache.jena.graph.Triple): List[Node] = {
    var vars = List[Node]()

    if(tp.getSubject.isVariable) {
      vars  = vars :+ tp.getSubject
    }

    if(tp.getPredicate.isVariable) {
      vars  = vars :+ tp.getPredicate
    }

    if(tp.getObject.isVariable) {
      vars  = vars :+ tp.getObject
    }

    vars
  }

  /**
    * Returns `true` if `rule1 has the same body as `rule2`, otherwise `false`.
    */
  def sameBody(rule1: Rule, rule2: Rule): Boolean = {
    GraphUtils.areIsomorphic(graphOfBody(rule1), graphOfBody(rule2))
  }

  /**
    * Returns `true` if `rule1 has the same head as `rule2`, otherwise `false`.
    */
  def sameHead(rule1: Rule, rule2: Rule): Boolean = {
    GraphUtils.areIsomorphic(graphOfHead(rule1), graphOfHead(rule2))
  }

  /**
    * Returns `true` if `rule1` and `rule2` can be merged, i.e. both rules denote the same input but
    * probably different output, otherwise `false`.
    */
  def canMerge(rule1: Rule, rule2: Rule): Boolean = {
    sameBody(rule1, rule2)
  }

  /**
    * Some convenience methods that can be called directly on a rule object.
    *
    * @param rule the rule
    */
  implicit class RuleExtension(val rule: Rule) {
    /**
      * Returns the triple patterns contained in the body of the rule.
      */
    def bodyTriplePatterns(): Seq[TriplePattern] = {
      rule.getBody.collect { case b: TriplePattern => b}
    }

    /**
      * Returns the triple patterns contained in the head of the rule.
      */
    def headTriplePatterns(): Seq[TriplePattern] = {
      rule.getHead.collect { case b: TriplePattern => b }
    }

    /**
      * Returns `true` if the rule has the same body as the other rule, otherwise `false`.
      */
    def sameBody(otherRule: Rule): Boolean = {
      RuleUtils.sameBody(rule, otherRule)
    }

    /**
      * Returns `true` if the rule has the same head as the other rule, otherwise `false`.
      */
    def sameHead(otherRule: Rule): Boolean = {
      RuleUtils.sameHead(rule, otherRule)
    }
  }

  implicit class TriplePatternEqualiltyExtension(val tp: TriplePattern) {
    def ==(that: TriplePatternEqualiltyExtension) = that.tp.asTriple().equals(this.tp.asTriple())

    override def equals(that: Any) = that match {
      case t: TriplePatternEqualiltyExtension => t.tp.asTriple().equals(this.tp.asTriple())
      case _ => false
    }
  }


}
