package net.sansa_stack.inference.rules.plan

import java.io.PrintWriter

import scala.collection.mutable

import com.google.common.collect.ImmutableList
import org.apache.calcite.config.Lex
import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.rules.{FilterJoinRule, ProjectJoinTransposeRule}
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.Frameworks.PlannerAction
import org.apache.calcite.tools.{Frameworks, RelBuilder, RuleSets}
import org.apache.jena.graph.Node
import org.apache.jena.reasoner.rulesys.Rule

import net.sansa_stack.inference.utils.{Logging, RuleUtils}
import net.sansa_stack.inference.utils.RuleUtils.RuleExtension
import net.sansa_stack.inference.utils.TripleUtils._

/**
  * @author Lorenz Buehmann
  */
class PlanGenerator extends Logging{

  val traitDefs: ImmutableList[RelTraitDef[_ <: RelTrait]] = ImmutableList.of(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)

  val optRuleSet = RuleSets.ofList(
    FilterJoinRule.FILTER_ON_JOIN,// push a filter into a join
    FilterJoinRule.JOIN,// push filter into the children of a join
    ProjectJoinTransposeRule.INSTANCE// push a projection to the children of a join
  )

  val calciteFrameworkConfig = Frameworks.newConfigBuilder()
    .parserConfig(SqlParser.configBuilder()
      // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
      // case when they are read, and whether identifiers are matched case-sensitively.
      .setLex(Lex.MYSQL)
      .build())
    // Sets the schema to use by the planner
    .defaultSchema(TriplesSchema.get())
    .traitDefs(traitDefs)
    // Context provides a way to store data within the planner session that can be accessed in planner rules.
    .context(Contexts.EMPTY_CONTEXT)
    // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
    .ruleSets(optRuleSet)
    // Custom cost factory to use during optimization
    .costFactory(null)
    .typeSystem(RelDataTypeSystem.DEFAULT)
    .build()

  // prepare planner and collect context instances
  val clusters: Array[RelOptCluster] = Array(null)
  val relOptSchemas: Array[RelOptSchema] = Array(null)
  val rootSchemas: Array[SchemaPlus] = Array(null)

  Frameworks.withPlanner(new PlannerAction[Void] {
    override def apply(
                        cluster: RelOptCluster,
                        relOptSchema: RelOptSchema,
                        rootSchema: SchemaPlus)
    : Void = {
      clusters(0) = cluster
      relOptSchemas(0) = relOptSchema
      rootSchemas(0) = rootSchema
      null
    }
  })

  val relBuilder = RelBuilder.create(calciteFrameworkConfig)

  val planner2: RelOptPlanner = clusters(0).getPlanner


  val planner = Frameworks.getPlanner(calciteFrameworkConfig)

  def generate(rule: Rule): RelNode = {
    info(s"Rule:\n$rule")

    val body = rule.bodyTriplePatterns().map(tp => tp.toTriple).toSet

    val visited = mutable.Set[org.apache.jena.graph.Triple]()

    //    process(body.head, body, visited)

    // group triple patterns by var
    val map = new mutable.HashMap[Node, collection.mutable.Set[org.apache.jena.graph.Triple]]() with mutable.MultiMap[Node, org.apache.jena.graph.Triple]
    body.foreach { tp =>
      val vars = RuleUtils.varsOf(tp)
      vars.foreach { v =>
        map.addBinding(v, tp)
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
    info(s"SQL Query:\n$sqlQuery")

    val sqlNode = planner.parse(sqlQuery)

    val validatedSqlNode = planner.validate(sqlNode)

    val relNode = planner.rel(validatedSqlNode).project()

//    // decorrelate
//    val decorPlan = RelDecorrelator.decorrelateQuery(relNode)
//
//    val optProgram = Programs.ofRules(optRuleSet)
//
//    val program = optProgram.run(planner2, decorPlan, relNode.getTraitSet)
//
//    program

    relNode
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
