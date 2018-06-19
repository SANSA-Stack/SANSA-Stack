package net.sansa_stack.inference.common

import java.nio.file.{Path, Paths}

import net.sansa_stack.inference.rules._
import net.sansa_stack.inference.rules.minimizer.DefaultRuleDependencyGraphMinimizer
import net.sansa_stack.inference.utils.GraphUtils._
import net.sansa_stack.inference.utils.RuleUtils

/**
  * Computes a given set of rules and exports its rule dependency graph before and after minimization.
  *
  *
  * @author Lorenz Buehmann
  */
object DependencyGraphTest {

  // the config object
  case class Config(in: Path = null,
                    out: Path = null,
                    profile: String = "",
                    ruleNames: Seq[String] = Seq()
                     )

  implicit val pathRead: scopt.Read[Path] =
    scopt.Read.reads(Paths.get(_))

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("DependencyGraphTest") {

    head("DependencyGraphTest", "0.1.0")

    opt[Path]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file containing the rules")

    opt[Path]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    opt[String]('p', "profile").required().valueName("<profile name>").
      action((x, c) => c.copy(profile = x)).
      text("the name of the set of rules to process - will be used for output files")

    opt[Seq[String]]("rules").optional().valueName("<ruleName1>,<ruleName2>,...").
      action((x, c) => {
        c.copy(ruleNames = x)
      }).
      text("list of rule names to process just a subset of the rules contained in the given input file")
  }

  def main(args: Array[String]): Unit = {

    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config)
      case None =>
        // scalastyle:off println
        println(parser.usage)
      // scalastyle:on println
    }
  }

  def run(config: Config): Unit = {

    // make output dirs
    config.out.toFile.mkdirs()

    // load the rules
    var rules = RuleUtils.load(config.in.toAbsolutePath.toString)

    // filter if necessary
    if(config.ruleNames.nonEmpty) {
      rules = rules.filter(r => config.ruleNames.contains(r.getName))
    }

//    val names = Seq("rdfp13a", "rdfp13b", "rdfp13c", "rdfs5", "rdfs7") // property rules
    val names = Seq("rdfp13a", "rdfp13b", "rdfp13c")// , "rdfs5", "rdfs7", "rdfp3", "rdfp4") // property rules + some instance rules
//    val names = Seq("rdfs5", "rdfs7", "rdfp3", "rdfp4") // property TC rule + some instance rules

    val minimizer = new DefaultRuleDependencyGraphMinimizer()

    // export graphs for each rule
    rules.foreach(rule => RuleUtils.asGraph(rule).export(config.out.resolve(s"rule_${rule.getName}.graphml").toAbsolutePath.toString))

    // generate the rule dependency graph
    var dependencyGraph = RuleDependencyGraphGenerator.generate(rules.toSet)
    dependencyGraph.export(config.out.resolve(s"rdg_${config.profile}.graphml").toAbsolutePath.toString, showInFlowDirection = true)

    // generate the minimized graph
    dependencyGraph = minimizer.execute(dependencyGraph) // RuleDependencyGraphGenerator.generate(rules, pruned = true)
    dependencyGraph.export(config.out.resolve(s"rdg_${config.profile}_minimized.graphml").toAbsolutePath.toString, showInFlowDirection = true)

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
