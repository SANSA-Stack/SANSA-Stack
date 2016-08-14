package org.sansa.inference.common

import org.sansa.inference.rules.{HighLevelRuleDependencyGraphGenerator, RuleDependencyGraph, RuleDependencyGraphGenerator, RuleSets}
import org.sansa.inference.utils.GraphUtils._

/**
  * @author Lorenz Buehmann
  */
object DependencyGraphTest {

  def main(args: Array[String]): Unit = {
    // define the rules
    val rules = RuleSets.RDFS_SIMPLE

    // generate the rule dependency graph
    val dependencyGraph = RuleDependencyGraphGenerator.generate(rules)
    dependencyGraph.export("/tmp/rdfs-simple.graphml")

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
