package net.sansa_stack.inference.spark.forwardchaining.triples

import net.sansa_stack.inference.data.Jena
import net.sansa_stack.inference.spark.data.model.RDFGraphNative
import net.sansa_stack.inference.spark.rules.RuleExecutorNative
import org.apache.jena.graph.Triple
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.language.{existentials, implicitConversions}

/**
  * A naive implementation of the forward chaining based reasoner that does fix-point iteration, i.e. it applies
  * all rules in each iteration until no new data has been generated.
  * .
  *
  * @author Lorenz Buehmann
  */
class ForwardRuleReasonerNaive(sc: SparkContext, rules: Set[Rule])
  extends AbstractForwardRuleReasoner[Jena, RDD[Triple], RDFGraphNative] {

  private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  val ruleExecutor = new RuleExecutorNative(sc)

  /**
    * Applies forward chaining to the given RDF graph and returns a new RDF graph that contains all additional
    * triples based on the underlying set of rules.
    *
    * @param graph the RDF graph
    * @return the materialized RDF graph
    */
  def apply(graph: RDFGraphNative): RDFGraphNative = {

    var currentGraph = graph

    var iteration = 0

    var oldCount = 0L
    var nextCount = currentGraph.size()
    do {
      iteration += 1
      logger.debug("Iteration " + iteration)
      oldCount = nextCount

      currentGraph = currentGraph.union(applyRules(graph)).distinct()
      currentGraph.cache()

      nextCount = currentGraph.size()
    } while (nextCount != oldCount)

    graph
  }

  /**
    * Apply a set of rules on the given graph.
    *
    * @param graph the graph
    */
  def applyRules(graph: RDFGraphNative): RDFGraphNative = {
    var newGraph = graph
    rules.foreach {rule =>
      newGraph = newGraph.union(applyRule(rule, graph))
    }
    newGraph
  }

  /**
    * Apply a single rule on the given graph.
    *
    * @param rule the rule
    * @param graph the graph
    */
  def applyRule(rule: Rule, graph: RDFGraphNative): RDFGraphNative = {
    logger.debug("Rule:" + rule)
    ruleExecutor.execute(rule, graph)
  }
}
