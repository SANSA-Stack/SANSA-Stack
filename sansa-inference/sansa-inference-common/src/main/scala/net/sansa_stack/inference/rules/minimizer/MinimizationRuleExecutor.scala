package net.sansa_stack.inference.rules.minimizer

import scala.jdk.CollectionConverters._

import com.google.common.util.concurrent.AtomicLongMap

import net.sansa_stack.inference.rules.RuleDependencyGraph
import net.sansa_stack.inference.utils.Logging

object MinimizationRuleExecutor {
  protected val timeMap = AtomicLongMap.create[String]()

  /** Resets statistics about time spent running specific rules */
  def resetTime(): Unit = timeMap.clear()

  /** Dump statistics about time spent running specific rules. */
  def dumpTimeSpent(): String = {
    val map = timeMap.asMap().asScala
    val maxSize = map.keys.map(_.toString.length).max
    map.toSeq.sortBy(_._2).reverseMap { case (k, v) =>
      s"${k.padTo(maxSize, " ").mkString} $v"
    }.mkString("\n", "\n", "")
  }
}

/**
  * @author Lorenz Buehmann
  */
abstract class MinimizationRuleExecutor extends Logging {

  /**
    * An execution strategy for rules that indicates the maximum number of executions. If the
    * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
    */
  abstract class Strategy { def maxIterations: Int }

  /** A strategy that only runs once. */
  case object Once extends Strategy { val maxIterations = 1 }

  /** A strategy that runs until fix point or maxIterations times, whichever comes first. */
  case class FixedPoint(maxIterations: Int) extends Strategy

  /** A batch of rules. */
  protected case class Batch(name: String, strategy: Strategy, rules: MinimizationRule*)

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  protected def batches: Seq[Batch]


  /**
    * Executes the batches of rules defined by the subclass. The batches are executed serially
    * using the defined execution strategy. Within each batch, rules are also executed serially.
    */
  def execute(graph: RuleDependencyGraph): RuleDependencyGraph = {
    var curGraph = graph

    batches.foreach { batch =>
      val batchStartGraph = curGraph
      var iteration = 1
      var lastGraph = curGraph
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      while (continue) {
        curGraph = batch.rules.foldLeft(curGraph) {
          case (graph, rule) =>
            debug(
              s"""
                 |=== Applying Rule ${rule.ruleName} ===
                """.stripMargin)
            val startTime = System.nanoTime()
            val result = rule(graph)
            val runTime = System.nanoTime() - startTime
            MinimizationRuleExecutor.timeMap.addAndGet(rule.ruleName, runTime)

            if (!result.equals(graph)) {
              trace(
                s"""
                   |=== Applying Rule ${rule.ruleName} ===
                """.stripMargin)
            }

            result
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            val message = s"Max iterations (${iteration - 1}) reached for batch ${batch.name}"
            warn(message)
          }
          continue = false
        }

        if (curGraph.equals(lastGraph)) {
          trace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastGraph = curGraph
      }

      if (!batchStartGraph.equals(curGraph)) {
        debug(
          s"""
             |=== Result of Batch ${batch.name} ===
        """.stripMargin)
      } else {
        trace(s"Batch ${batch.name} has no effect.")
      }
    }

    curGraph
  }

  override def debug(msg: => String): Unit = println(msg)
}
