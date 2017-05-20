package net.sansa_stack.inference.spark.forwardchaining

import scala.language.{existentials, implicitConversions}

import org.apache.jena.reasoner.rulesys.Rule
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import net.sansa_stack.inference.data.RDF
import net.sansa_stack.inference.rules.{HighLevelRuleDependencyGraphGenerator, RuleDependencyGraph, RuleDependencyGraphGenerator}
import net.sansa_stack.inference.spark.data.model.AbstractRDFGraphSpark
import net.sansa_stack.inference.spark.rules.RuleExecutor

/**
  * An optimized implementation of the forward chaining based reasoner.
  *
  * @author Lorenz Buehmann
  */
abstract class ForwardRuleReasonerOptimized[D[T], N <: RDF#Node, T <: RDF#Triple, G <: AbstractRDFGraphSpark[D, N, T, G]]
(sparkSession: SparkSession, rules: Set[Rule], ruleExecutor: RuleExecutor[D, N, T, G])
  extends AbstractForwardRuleReasoner[D, N, T, G] {

  private val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(this.getClass.getName))

  var ruleExecutionCnt = 0
  var countCnt = 0
  var unionCnt = 0
  var distinctCnt = 0

  def reset(): Unit = {
    ruleExecutionCnt = 0
    countCnt = 0
    unionCnt = 0
    distinctCnt = 0
  }

  def showExecutionStats(): Unit = {
    info("#Executed Rules:" + ruleExecutionCnt)
    info("#Count Request:" + countCnt)
    info("#Union Request:" + unionCnt)
    info("#Distinct Request:" + distinctCnt)
  }
  /**
    * Applies forward chaining to the given RDF graph and returns a new RDF graph that contains all additional
    * triples based on the underlying set of rules.
    *
    * @param graph the RDF graph
    * @return the materialized RDF graph
    */
  def apply(graph: G): G = {
    reset()

    var newGraph = graph.cache()

    // generate the rule dependency graph
    val dependencyGraph = RuleDependencyGraphGenerator.generate(rules, pruned = true)

    // generate the high-level dependency graph
    val highLevelDependencyGraph = HighLevelRuleDependencyGraphGenerator.generate(dependencyGraph)

    // apply topological sort and get the layers
    val layers = highLevelDependencyGraph.layers().foldLeft(List[(Int, scala.Iterable[RuleDependencyGraph])]())((b, a) => a :: b)

    // each layer contains a set of rule dependency graphs
    // for each layer we process those
    layers foreach { layer =>
      newGraph = newGraph
        .union(processLayer(layer, newGraph))
        .distinct()
        .cache()
      unionCnt += 1
      distinctCnt += 1
    }

    // de-duplicate
//    newGraph = newGraph.distinct()

    // return new graph
    newGraph
  }

  private def processLayer(layer: (Int, Iterable[RuleDependencyGraph]), graph: G): G = {
    logger.info("Processing layer " + layer._1 + "---" * 10)
    logger.info(layer._2.map(rdg => rdg.printNodes()).mkString("--"))

    var newGraph = graph

    val processedRDGs = layer._2.map{rdg =>
      logger.info("Processing dependency graph " + rdg.printNodes())
      applyRules(rdg, newGraph)
    }

    newGraph = newGraph.unionAll(processedRDGs.toSeq).distinct().cache()
    unionCnt += 1
    distinctCnt += 1

    newGraph
  }

  /**
    * Apply the set of rules on the given graph by doing fix-point iteration.
    *
    * @param rdg the rule dependency graph
    * @param graph the RDF graph
    */
  def applyRules(rdg: RuleDependencyGraph, graph: G): G = {
    var newGraph = graph.cache()

    val rules = rdg.rules().toSeq

    if(rdg.hasCycle()) {
      var newGraph = graph.cache()
      var iteration = 1
      var oldCount = 0L
      var nextCount = newGraph.size()
      logger.info(s"initial size:$nextCount")

      do {
        logger.info("Iteration " + iteration)
        iteration += 1
        oldCount = nextCount

        newGraph = newGraph.union(applyRulesOnce(rules, newGraph)).distinct().cache()
        unionCnt += 1
        distinctCnt += 1

        nextCount = newGraph.size()
        logger.info(s"new size:$nextCount")
        countCnt += 1
      } while (nextCount != oldCount)
    } else {
      newGraph = newGraph.union(applyRulesOnce(rules, newGraph))
    }

    newGraph
  }

  /**
    * Apply the set of rules on the given graph by doing fix-point iteration.
    *
    * @param rules the rules
    * @param graph the graph
    */
  def applyRules(rules: Seq[Rule], graph: G): G = {
    var newGraph = graph.cache()
    var iteration = 1
    var oldCount = 0L
    var nextCount = newGraph.size()
    logger.info(s"initial size:$nextCount")
    do {
      logger.info("Iteration " + iteration)
      iteration += 1
      oldCount = nextCount

      newGraph = newGraph.union(applyRulesOnce(rules, newGraph)).distinct().cache()
      unionCnt += 1
      distinctCnt += 1

      nextCount = newGraph.size()
      logger.info(s"new size:$nextCount")
      countCnt += 1
    } while (nextCount != oldCount)

    newGraph
  }

  /**
    * Apply the set of rules on the given graph once.
    *
    * @param rules the rules
    * @param graph the graph
    */
  def applyRulesOnce(rules: Seq[Rule], graph: G): G = {
    val graphs = rules.map {rule =>
      applyRule(rule, graph)
    }
    val newGraph = graph.unionAll(graphs.toList)
      unionCnt += 1
//    println(newGraph.toRDD().toDebugString)
    newGraph

  }

  /**
    * Apply a single rule on the given graph.
    *
    * @param rule the rule
    * @param graph the graph
    */
  def applyRule(rule: Rule, graph: G): G = {
    logger.debug("Applying rule:" + rule)
    ruleExecutionCnt += 1
    ruleExecutor.execute(rule, graph)
  }
}
