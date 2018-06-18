package net.sansa_stack.inference.common

import net.sansa_stack.inference.rules._
import net.sansa_stack.inference.rules.minimizer.DefaultRuleDependencyGraphMinimizer
import net.sansa_stack.inference.utils.GraphUtils._
import net.sansa_stack.inference.utils.RuleUtils

/**
  * @author Lorenz Buehmann
  */
object DependencyGraphTest {

  def main(args: Array[String]): Unit = {

    val path = "/tmp"

//    val names = Seq("rdfp13a", "rdfp13b", "rdfp13c", "rdfs5", "rdfs7") // property rules
    val names = Seq("rdfp13a", "rdfp13b", "rdfp13c", "rdfs5", "rdfs7", "rdfp3", "rdfp4") // property rules + some instance rules
//    val names = Seq("rdfs5", "rdfs7", "rdfp3", "rdfp4") // property TC rule + some instance rules

    // define the rules
    val rules = RuleSets.OWL_HORST // .filter(r => names.contains(r.getName))
    val profile = ReasoningProfile.OWL_HORST
//    val rules = RuleSets.RDFS_SIMPLE
//    val profile = ReasoningProfile.RDFS_SIMPLE

    val minimizer = new DefaultRuleDependencyGraphMinimizer()

    // export graphs
    rules.foreach(rule => RuleUtils.asGraph(rule).export(s"${path}/rule-${rule.getName}.graphml"))

    // generate the rule dependency graph
    var dependencyGraph = RuleDependencyGraphGenerator.generate(rules)
    dependencyGraph.export(s"${path}/rdg-${profile}.graphml")

    dependencyGraph = minimizer.execute(dependencyGraph) // RuleDependencyGraphGenerator.generate(rules, pruned = true)
    dependencyGraph.export(s"${path}/rdg-${profile}-pruned.graphml")
//    dependencyGraph.exportAsPDF(s"${path}/rdg-${profile}-pruned.pdf")

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
      println(rdg.isCyclic)
      rdg.nodes.foreach(n => println(n findSuccessor (_.outDegree >= 1)))
    }
  }
}
