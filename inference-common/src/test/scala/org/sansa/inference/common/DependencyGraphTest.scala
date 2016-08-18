package org.sansa.inference.common

import org.sansa.inference.rules.{HighLevelRuleDependencyGraphGenerator, RuleDependencyGraph, RuleDependencyGraphGenerator, RuleSets}
import org.sansa.inference.utils.GraphUtils._
import org.sansa.inference.utils.RuleUtils

/**
  * @author Lorenz Buehmann
  */
object DependencyGraphTest {

  def main(args: Array[String]): Unit = {

    val path = "/tmp"

    val names = Seq("rdfp13a", "rdfp13b", "rdfp13c", "rdfs5", "rdfs7")

    // define the rules
    val rules = RuleSets.OWL_HORST.filter(r => names.contains(r.getName))

    // export graphs
    rules.foreach(rule => RuleUtils.asGraph(rule).export(s"${path}/rule-${rule.getName}.graphml"))

    // generate the rule dependency graph
    var dependencyGraph = RuleDependencyGraphGenerator.generate(rules)
    dependencyGraph.export(s"${path}/rdg-rdfs-simple.graphml")

    dependencyGraph = RuleDependencyGraphGenerator.generate(rules, pruned = true)
    dependencyGraph.export(s"${path}/rdg-rdfs-simple-pruned.graphml")

    // generate the high-level dependency graph
    val highLevelDependencyGraph = HighLevelRuleDependencyGraphGenerator.generate(dependencyGraph)

    // apply topological sort and get the layers
    val layers = highLevelDependencyGraph.layers()

    // each layer contains a set of rule dependency graphs
    // for each layer we process those
    layers foreach showLayer
  }

  private def showLayer(layer: (Int, Iterable[RuleDependencyGraph])): Unit = {
    println("Processing layer " + layer._1 + "---" * 10)
    println(layer._2.map(rdg => rdg.printNodes()).mkString("--"))

    layer._2.foreach{rdg =>
      println("Processing dependency graph " + rdg.printNodes())
    }
  }
}
