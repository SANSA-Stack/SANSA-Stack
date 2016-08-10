package org.sansa.inference.rules.plan

import java.io.PrintWriter

import org.apache.calcite.config.Lex
import org.apache.calcite.plan.{Contexts, ConventionTraitDef}
import org.apache.calcite.rel.RelCollationTraitDef
import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.rules.FilterJoinRule.FilterIntoJoinRule
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{Frameworks, RuleSets}
import org.apache.jena.graph.Node
import org.apache.jena.reasoner.rulesys.Rule
import org.sansa.inference.utils.RuleUtils
import org.sansa.inference.utils.RuleUtils.RuleExtension
import org.sansa.inference.utils.TripleUtils._

import scala.collection.mutable

/**
  * @author Lorenz Buehmann
  */
class PlanGenerator {

  val traitDefs = List(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)

  val calciteFrameworkConfig = Frameworks.newConfigBuilder()
    .parserConfig(SqlParser.configBuilder()
      // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
      // case when they are read, and whether identifiers are matched case-sensitively.
      .setLex(Lex.MYSQL)
      .build())
    // Sets the schema to use by the planner
    .defaultSchema(TriplesSchema.get())
//    .traitDefs(traitDefs)
    // Context provides a way to store data within the planner session that can be accessed in planner rules.
    .context(Contexts.EMPTY_CONTEXT)
    // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
    .ruleSets(RuleSets.ofList(new FilterIntoJoinRule()))
    // Custom cost factory to use during optimization
    .costFactory(null)
    .typeSystem(RelDataTypeSystem.DEFAULT)
    .build()

  val planner = Frameworks.getPlanner(calciteFrameworkConfig)

  def generate(rule: Rule) = {
    println(s"Rule:\n$rule")

    val body = rule.bodyTriplePatterns().map(tp => tp.toTriple).toSet

    val visited = mutable.Set[org.apache.jena.graph.Triple]()

    //    process(body.head, body, visited)

    // group triple patterns by var
    val map = new mutable.HashMap[Node, collection.mutable.Set[org.apache.jena.graph.Triple]] () with mutable.MultiMap[Node, org.apache.jena.graph.Triple]
    body.foreach{tp =>
      val vars = RuleUtils.varsOf(tp)
      vars.foreach{v =>
        map.addBinding(v,tp)
      }
    }

    val joins = new mutable.HashSet[Join]

    map.foreach{e =>
      val v = e._1
      val tps = e._2.toList.sortBy(_.toString).combinations(2).foreach(c =>
        joins.add(new Join(c(0), c(1), v))
      )
    }

    val sqlQuery = new Plan(body, rule.headTriplePatterns().toList.head.asTriple(), joins).toSQL
    println(s"SQL Query:\n$sqlQuery")

    val sqlNode = planner.parse(sqlQuery)

    val validatedSqlNode = planner.validate(sqlNode)

    val logicalPlan = planner.rel(validatedSqlNode)

    logicalPlan.project()
  }



}

object PlanGenerator {

  def main(args: Array[String]): Unit = {

    val planGenerator = new PlanGenerator()

    val rules = RuleUtils.load("rules/rdfs-simple.rules")

    val plan = planGenerator.generate(rules.head)

    plan.explain(new RelWriterImpl(new PrintWriter(System.out)))
  }
}
